// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

var _ resolver = (*k8sResolver)(nil)

var (
	errNoSvc                        = errors.New("no service specified to resolve the backends")
	errNoSvcPorts                   = errors.New("no ports specified to resolve the backends")
	k8sResolverMutator              = tag.Upsert(tag.MustNewKey("resolver"), "k8s service")
	k8sResolverSuccessTrueMutators  = []tag.Mutator{resolverMutator, successTrueMutator}
	k8sResolverSuccessFalseMutators = []tag.Mutator{resolverMutator, successFalseMutator}
)

type k8sResolver struct {
	logger    *zap.Logger
	service   string
	namespace string
	port      []int32

	handler        *handler
	once           *sync.Once
	epsListWatcher cache.ListerWatcher
	endpointsStore *sync.Map

	endpoints         []string
	onChangeCallbacks []func([]string)

	stopCh             chan (struct{})
	updateLock         sync.Mutex
	shutdownWg         sync.WaitGroup
	changeCallbackLock sync.RWMutex
}

func newK8sResolver(clt kubernetes.Interface,
	logger *zap.Logger,
	service string,
	ports []int32) (*k8sResolver, error) {

	if len(service) == 0 {
		return nil, errNoSvc
	}
	if len(ports) == 0 {
		return nil, errNoSvcPorts
	}

	nAddr := strings.SplitN(service, ".", 2)
	name, namespace := nAddr[0], "default"
	if len(nAddr) > 1 {
		namespace = nAddr[1]
	}

	epsSelector := fmt.Sprintf("metadata.name=%s", name)
	epsListWatcher := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = epsSelector
			return clt.CoreV1().Endpoints(namespace).List(context.TODO(), options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = epsSelector
			return clt.CoreV1().Endpoints(namespace).Watch(context.TODO(), options)
		},
	}

	epsStore := &sync.Map{}
	h := &handler{endpoints: epsStore, logger: logger}
	r := &k8sResolver{
		logger:         logger,
		service:        name,
		namespace:      namespace,
		port:           ports,
		once:           &sync.Once{},
		endpointsStore: epsStore,
		epsListWatcher: epsListWatcher,
		handler:        h,
		stopCh:         make(chan struct{}),
	}
	h.callback = r.resolve

	return r, nil
}

func (r *k8sResolver) start(ctx context.Context) error {
	if r.once == nil {
		return fmt.Errorf("cannot Start() partial k8sResolver (nil *sync.Once)")
	}
	if r.handler == nil {
		return fmt.Errorf("cannot Start() partial k8sResolver (nil *handler)")
	}
	var initErr error
	r.once.Do(func() {
		if r.epsListWatcher != nil {
			r.logger.Debug("creating and starting endpoints informer")
			epsInformer := cache.NewSharedInformer(r.epsListWatcher, &corev1.Endpoints{}, 0)
			if _, err := epsInformer.AddEventHandler(r.handler); err != nil {
				r.logger.Error("error adding event handler to endpoints informer", zap.Error(err))
			}
			go epsInformer.Run(r.stopCh)
			if !cache.WaitForCacheSync(r.stopCh, epsInformer.HasSynced) {
				r.logger.Error("error endpoints informer not sync")
				initErr = errors.New("endpoints informer not sync")
				return
			}
		}
	})
	if initErr != nil {
		return initErr
	}

	r.logger.Debug("K8s service resolver started",
		zap.String("service", r.service),
		zap.String("namespace", r.namespace),
		zap.Int32s("ports", r.port))
	return nil
}

func (r *k8sResolver) shutdown(ctx context.Context) error {
	r.changeCallbackLock.Lock()
	r.onChangeCallbacks = nil
	r.changeCallbackLock.Unlock()

	close(r.stopCh)
	r.shutdownWg.Wait()
	return nil
}
func newInClusterClient() (kubernetes.Interface, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func (r *k8sResolver) resolve(ctx context.Context) ([]string, error) {
	r.shutdownWg.Add(1)
	defer r.shutdownWg.Done()

	var backends []string
	r.endpointsStore.Range(func(address, value any) bool {
		addr := address.(string)
		var backend string
		ip, err := net.ResolveIPAddr("ip", addr)
		if err != nil {
			stats.RecordWithTags(ctx, k8sResolverSuccessFalseMutators, mNumResolutions.M(1))
			r.logger.Error("resolve address error", zap.String("address", addr), zap.Error(err))
			return true
		}
		stats.RecordWithTags(ctx, k8sResolverSuccessTrueMutators, mNumResolutions.M(1))
		if ip.IP.To4() != nil {
			backend = ip.String()
		} else {
			// it's an IPv6 address
			backend = fmt.Sprintf("[%s]", ip.String())
		}

		for _, port := range r.port {
			// if a port is specified in the configuration, add it
			if port != 0 {
				backends = append(backends, fmt.Sprintf("%s:%d", backend, port))
			}
		}
		return true
	})

	// keep it always in the same order
	sort.Strings(backends)

	if equalStringSlice(r.endpoints, backends) {
		return r.endpoints, nil
	}

	// the list has changed!
	r.updateLock.Lock()
	r.endpoints = backends
	r.updateLock.Unlock()
	_ = stats.RecordWithTags(ctx, resolverSuccessTrueMutators, mNumBackends.M(int64(len(backends))))

	// propagate the change
	r.changeCallbackLock.RLock()
	for _, callback := range r.onChangeCallbacks {
		callback(r.endpoints)
	}
	r.changeCallbackLock.RUnlock()
	return r.endpoints, nil
}

func (r *k8sResolver) onChange(f func([]string)) {
	r.changeCallbackLock.Lock()
	defer r.changeCallbackLock.Unlock()
	r.onChangeCallbacks = append(r.onChangeCallbacks, f)
}

var _ cache.ResourceEventHandler = (*handler)(nil)

type handler struct {
	endpoints *sync.Map
	callback  func(ctx context.Context) ([]string, error)
	logger    *zap.Logger
}

func (h handler) OnAdd(obj interface{}, isInInitialList bool) {
	h.logger.Debug("onAdd called")
	var endpoints []string
	switch object := obj.(type) {
	case *corev1.Endpoints:
		endpoints = convertToEndpoints(object)
	default: // unsupported
		return
	}
	changed := false
	for _, ep := range endpoints {
		if _, loaded := h.endpoints.LoadOrStore(ep, ep); !loaded {
			changed = true
		}
	}
	h.logger.Debug("onAdd check", zap.Bool("changed", changed))
	if changed {
		h.callback(context.TODO())
	}
}

func (h handler) OnUpdate(oldObj, newObj interface{}) {
	h.logger.Debug("onUpdate called")
	switch oldObj.(type) {
	case *corev1.Endpoints:
		newEps, ok := newObj.(*corev1.Endpoints)
		if !ok {
			return
		}
		endpoints := convertToEndpoints(newEps)
		changed := false
		for _, ep := range endpoints {
			if _, loaded := h.endpoints.LoadOrStore(ep, ep); !loaded {
				changed = true
			}
		}
		h.logger.Debug("onUpdate check", zap.Bool("changed", changed))
		if changed {
			h.callback(context.TODO())
		}
	default: // unsupported
		return
	}
}

func (h handler) OnDelete(obj interface{}) {
	h.logger.Debug("onDelete called")
	var endpoints []string
	switch object := obj.(type) {
	case *cache.DeletedFinalStateUnknown:
		h.OnDelete(object.Obj)
		return
	case *corev1.Endpoints:
		if object != nil {
			endpoints = convertToEndpoints(object)
		}
	default: // unsupported
		return
	}
	if len(endpoints) != 0 {
		for _, endpoint := range endpoints {
			h.endpoints.Delete(endpoint)
		}
		h.logger.Debug("onDelete check", zap.Bool("changed", true))
		h.callback(context.TODO())
	}
}
func convertToEndpoints(eps ...*corev1.Endpoints) []string {
	var ipAddress []string
	for _, ep := range eps {
		for _, subsets := range ep.Subsets {
			for _, addr := range subsets.Addresses {
				ipAddress = append(ipAddress, addr.IP)
			}
		}
	}
	return ipAddress
}
