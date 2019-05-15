/*
Copyright 2016 The Rook Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package client to manage a rook client.
package client

import (
	"fmt"
	"reflect"

	"github.com/coreos/pkg/capnslog"
	opkit "github.com/rook/operator-kit"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	ceph "github.com/rook/rook/pkg/daemon/ceph/client"
	cephconfig "github.com/rook/rook/pkg/daemon/ceph/config"
	"github.com/rook/rook/pkg/daemon/ceph/model"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/client-go/tools/cache"
)

var logger = capnslog.NewPackageLogger("github.com/rook/rook", "op-client")

// ClientResource represents the Client custom resource object
var ClientResource = opkit.CustomResource{
	Name:    "cephclient",
	Plural:  "cephclients",
	Group:   cephv1.CustomResourceGroup,
	Version: cephv1.Version,
	Scope:   apiextensionsv1beta1.NamespaceScoped,
	Kind:    reflect.TypeOf(cephv1.CephClient{}).Name(),
}

// ClientController represents a controller object for client custom resources
type ClientController struct {
	context   *clusterd.Context
	namespace string
}

// NewClientController create controller for watching client custom resources created
func NewClientController(context *clusterd.Context, namespace string) *ClientController {
	return &ClientController{
		context:   context,
		namespace: namespace,
	}
}

// Watch watches for instances of Client custom resources and acts on them
func (c *ClientController) StartWatch(stopCh chan struct{}) error {

	resourceHandlerFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onAdd,
		UpdateFunc: c.onUpdate,
		DeleteFunc: c.onDelete,
	}

	logger.Infof("start watching client resources in namespace %s", c.namespace)
	watcher := opkit.NewWatcher(ClientResource, c.namespace, resourceHandlerFuncs, c.context.RookClientset.CephV1().RESTClient())
	go watcher.Watch(&cephv1.CephClient{}, stopCh)

	return nil
}

func (c *ClientController) onAdd(obj interface{}) {
	logger.Infof("new client resource added")
	/*err = createPool(c.context, client)
	if err != nil {
		logger.Errorf("failed to create client %s. %+v", client.ObjectMeta.Name, err)
	}*/
}

func (c *ClientController) onUpdate(oldObj, newObj interface{}) {
	oldPool, err := getClientObject(oldObj)
	if err != nil {
		logger.Errorf("failed to get old client object: %+v", err)
		return
	}
	client, err := getClientObject(newObj)
	if err != nil {
		logger.Errorf("failed to get new client object: %+v", err)
		return
	}

	if oldPool.Name != client.Name {
		logger.Errorf("failed to update client %s. name update not allowed", client.Name)
		return
	}
	if !clientChanged(oldPool.Spec, client.Spec) {
		logger.Debugf("client %s not changed", client.Name)
		return
	}

	// if the client is modified, allow the client to be created if it wasn't already
	/*logger.Infof("updating client %s", client.Name)
	if err := createPool(c.context, client); err != nil {
		logger.Errorf("failed to create (modify) client %s. %+v", client.ObjectMeta.Name, err)
	}*/
}

func (c *ClientController) ParentClusterChanged(cluster cephv1.ClusterSpec, clusterInfo *cephconfig.ClusterInfo) {
	logger.Debugf("No need to update the client after the parent cluster changed")
}

func clientChanged(old, new cephv1.ClientSpec) bool {
	/*if old.Replicated.Size != new.Replicated.Size {
		logger.Infof("client replication changed from %d to %d", old.Replicated.Size, new.Replicated.Size)
		return true
	}*/
	return false
}

func (c *ClientController) onDelete(obj interface{}) {
	logger.Infof("Going to remove client object")
	client, err := getClientObject(obj)
	if err != nil {
		logger.Errorf("failed to get client object: %+v", err)
		return
	}

	if err := deletePool(c.context, client); err != nil {
		logger.Errorf("failed to delete client %s. %+v", client.ObjectMeta.Name, err)
	}
}

// Create the client
/*func createPool(context *clusterd.Context, p *cephv1.CephBlockPool) error {
	// validate the client settings
	if err := ValidatePool(context, p); err != nil {
		return fmt.Errorf("invalid client %s arguments. %+v", p.Name, err)
	}

	// create the client
	logger.Infof("creating client %s in namespace %s", p.Name, p.Namespace)
	if err := ceph.CreatePoolWithProfile(context, p.Namespace, *p.Spec.ToModel(p.Name), clientApplicationNameRBD); err != nil {
		return fmt.Errorf("failed to create client %s. %+v", p.Name, err)
	}

	logger.Infof("created client %s", p.Name)
	return nil
}*/

// Delete the client
func deletePool(context *clusterd.Context, p *cephv1.CephClient) error {

	/*if err := ceph.DeletePool(context, p.Namespace, p.Name); err != nil {
		return fmt.Errorf("failed to delete client '%s'. %+v", p.Name, err)
	}*/

	return nil
}

// Check if the client exists
func clientExists(context *clusterd.Context, p *cephv1.CephClient) (bool, error) {
	clients, err := ceph.GetPools(context, p.Namespace)
	if err != nil {
		return false, err
	}
	for _, client := range clients {
		if client.Name == p.Name {
			return true, nil
		}
	}
	return false, nil
}

func ModelToSpec(client model.Pool) cephv1.PoolSpec {
	ec := client.ErasureCodedConfig
	return cephv1.PoolSpec{
		FailureDomain: client.FailureDomain,
		CrushRoot:     client.CrushRoot,
		Replicated:    cephv1.ReplicatedSpec{Size: client.ReplicatedConfig.Size},
		ErasureCoded:  cephv1.ErasureCodedSpec{CodingChunks: ec.CodingChunkCount, DataChunks: ec.DataChunkCount, Algorithm: ec.Algorithm},
	}
}

// Validate the client arguments
func ValidatePool(context *clusterd.Context, p *cephv1.CephClient) error {
	if p.Name == "" {
		return fmt.Errorf("missing name")
	}
	if p.Namespace == "" {
		return fmt.Errorf("missing namespace")
	}
	if err := ValidateClientSpec(context, p.Namespace, &p.Spec); err != nil {
		return err
	}
	return nil
}

func ValidateClientSpec(context *clusterd.Context, namespace string, p *cephv1.ClientSpec) error {
	/*if p.Replication() != nil && p.ErasureCode() != nil {
		return fmt.Errorf("both replication and erasure code settings cannot be specified")
	}
	if p.Replication() == nil && p.ErasureCode() == nil {
		return fmt.Errorf("neither replication nor erasure code settings were specified")
	}

	var crush ceph.CrushMap
	var err error
	if p.FailureDomain != "" || p.CrushRoot != "" {
		crush, err = ceph.GetCrushMap(context, namespace)
		if err != nil {
			return fmt.Errorf("failed to get crush map. %+v", err)
		}
	}

	// validate the failure domain if specified
	if p.FailureDomain != "" {
		found := false
		for _, t := range crush.Types {
			if t.Name == p.FailureDomain {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unrecognized failure domain %s", p.FailureDomain)
		}
	}

	// validate the crush root if specified
	if p.CrushRoot != "" {
		found := false
		for _, t := range crush.Buckets {
			if t.Name == p.CrushRoot {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unrecognized crush root %s", p.CrushRoot)
		}
	}
	*/
	return nil
}

func getClientObject(obj interface{}) (client *cephv1.CephClient, err error) {
	var ok bool
	client, ok = obj.(*cephv1.CephClient)
	if ok {
		// the client object is of the latest type, simply return it
		return client.DeepCopy(), nil
	}

	return nil, fmt.Errorf("not a known client object: %+v", obj)
}
