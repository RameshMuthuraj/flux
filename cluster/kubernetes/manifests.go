package kubernetes

import (
	kresource "github.com/weaveworks/flux/cluster/kubernetes/resource"
	"github.com/weaveworks/flux/image"
	"github.com/weaveworks/flux/resource"
)

type Manifests struct {
}

// FindDefinedServices implementation in files.go

func (c *Manifests) LoadManifests(paths ...string) (map[string]resource.Resource, error) {
	return kresource.Load(paths...)
}

func (c *Manifests) ParseManifests(allDefs []byte) (map[string]resource.Resource, error) {
	mans, err := kresource.ParseMultidoc(allDefs, "exported")
	return mans, err
}

func (c *Manifests) UpdateDefinition(def []byte, container string, image image.Ref) ([]byte, error) {
	return updatePodController(def, container, image)
}

// UpdatePolicies and ServicesWithPolicies in policies.go
