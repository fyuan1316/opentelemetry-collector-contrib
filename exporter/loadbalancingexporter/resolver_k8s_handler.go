package loadbalancingexporter

import (
	"context"
	"sync"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

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
		h.logger.Warn("onAdd unable to handle object", zap.Any("obj", obj))
		return
	}
	changed := false
	for _, ep := range endpoints {
		if _, loaded := h.endpoints.LoadOrStore(ep, true); !loaded {
			changed = true
		}
	}
	h.logger.Debug("onAdd check", zap.Bool("changed", changed))
	if changed {
		h.callback(context.Background())
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
			if _, loaded := h.endpoints.LoadOrStore(ep, true); !loaded {
				changed = true
			}
		}
		h.logger.Debug("onUpdate check", zap.Bool("changed", changed))
		if changed {
			h.callback(context.Background())
		}
	default: // unsupported
		h.logger.Warn("onUpdate unable to handle object", zap.Any("obj", oldObj))
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
		h.logger.Warn("onDelete unable to handle object", zap.Any("obj", obj))
		return
	}
	if len(endpoints) != 0 {
		for _, endpoint := range endpoints {
			h.endpoints.Delete(endpoint)
		}
		h.logger.Debug("onDelete check", zap.Bool("changed", true))
		h.callback(context.Background())
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
