// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestK8sResolve(t *testing.T) {
	// prepare
	service, defaultNs, ports := "lb", "default", []int32{8080, 9090}
	endpoint := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      service,
			Namespace: defaultNs,
		},
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{
					{IP: "192.168.10.100"},
				},
			},
		},
	}
	expectInit := []string{
		"192.168.10.100:8080",
		"192.168.10.100:9090",
	}

	cl := fake.NewSimpleClientset(endpoint)
	res, err := newK8sResolver(cl, zap.NewNop(), service, ports)
	require.NoError(t, err)

	// step-1 initialize
	require.NoError(t, res.start(context.Background()))
	defer func() {
		require.NoError(t, res.shutdown(context.Background()))
	}()
	_, err = res.resolve(context.Background()) // second resolution, should be noop

	// verify endpoints should be the same as expectInit
	assert.NoError(t, err)
	assert.Equal(t, expectInit, res.endpoints)

	// step-2 update, simulate changes to the backend ip address
	exist := endpoint.DeepCopy()
	endpoint.Subsets = append(endpoint.Subsets, corev1.EndpointSubset{
		Addresses: []corev1.EndpointAddress{{IP: "10.10.0.11"}},
	})
	patch := client.MergeFrom(exist)
	data, err := patch.Data(endpoint)
	require.NoError(t, err)
	_, err = cl.CoreV1().Endpoints(defaultNs).
		Patch(context.TODO(), service, types.MergePatchType, data, metav1.PatchOptions{})
	require.NoError(t, err)

	// verify update
	assert.Eventually(t, func() bool {
		_, err := res.resolve(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"10.10.0.11:8080",
			"10.10.0.11:9090",
			"192.168.10.100:8080",
			"192.168.10.100:9090",
		}, res.endpoints)
		return true
	}, time.Second, 20*time.Millisecond)

	// step-3 delete, simulate deletion of backends
	err = cl.CoreV1().Endpoints(defaultNs).
		Delete(context.TODO(), service, metav1.DeleteOptions{})
	require.NoError(t, err)

	// verify delete
	assert.Eventually(t, func() bool {
		_, err := res.resolve(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(res.endpoints))
		return true
	}, time.Second, 20*time.Millisecond)
}

func Test_newK8sResolver(t *testing.T) {
	type args struct {
		logger  *zap.Logger
		service string
		ports   []int32
	}
	tests := []struct {
		name          string
		args          args
		wantNil       bool
		wantErr       error
		wantService   string
		wantNamespace string
	}{
		{
			name: "invalid name of k8s service",
			args: args{
				logger:  zap.NewNop(),
				service: "",
				ports:   []int32{8080},
			},
			wantNil: true,
			wantErr: errNoSvc,
		},
		{
			name: "invalid ports of k8s service",
			args: args{
				logger:  zap.NewNop(),
				service: "lb.kube-public",
			},
			wantNil: true,
			wantErr: errNoSvcPorts,
		},
		{
			name: "use `default` namespace if namespace is not specified",
			args: args{
				logger:  zap.NewNop(),
				service: "lb",
				ports:   []int32{8080},
			},
			wantNil:       false,
			wantErr:       nil,
			wantService:   "lb",
			wantNamespace: "default",
		},
		{
			name: "use specified namespace",
			args: args{
				logger:  zap.NewNop(),
				service: "lb.kube-public",
				ports:   []int32{8080},
			},
			wantNil:       false,
			wantErr:       nil,
			wantService:   "lb",
			wantNamespace: "kube-public",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newK8sResolver(fake.NewSimpleClientset(), tt.args.logger, tt.args.service, tt.args.ports)
			if tt.wantErr != nil {
				require.Error(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantNil, got == nil)
				if !tt.wantNil {
					require.Equal(t, tt.wantService, got.service)
					require.Equal(t, tt.wantNamespace, got.namespace)
				}
			}
		})
	}
}
