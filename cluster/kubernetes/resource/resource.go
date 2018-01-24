package resource

import (
	"strings"

	yaml "gopkg.in/yaml.v2"

	"github.com/weaveworks/flux"
	fluxerr "github.com/weaveworks/flux/errors"
	"github.com/weaveworks/flux/policy"
	"github.com/weaveworks/flux/resource"
)

const (
	PolicyPrefix = "flux.weave.works/"
)

// -- unmarshaling code for specific object and field types

// struct to embed in objects, to provide default implementation
// type BaseObject struct {
// 	source string
// 	bytes  []byte
// 	Kind   string `yaml:"kind"`
// 	Meta   struct {
// 		Namespace   string            `yaml:"namespace"`
// 		Name        string            `yaml:"name"`
// 		Annotations map[string]string `yaml:"annotations,omitempty"`
// 	} `yaml:"metadata"`
// }
type Metadata struct {
	Name        string            `yaml:"name"`
	Annotations map[string]string `yaml:"annotations"`
	Namespace   string            `yaml:"namespace"`
}

type Container struct {
	Name  string `yaml:"name"`
	Image string `yaml:"image"`
}

type BaseObject struct {
	source   string
	bytes    []byte
	Metadata Metadata `yaml:"metadata"`
	Kind     string   `yaml:"kind"`
	Spec     struct {
		Template struct {
			Spec struct {
				Containers []Container `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
		JobTemplate struct {
			Spec struct {
				Template struct {
					Spec struct {
						Containers []Container `yaml:"containers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		} `yaml:"jobTemplate"`
	} `yaml:"spec"`
}

func (m Metadata) AnnotationsOrNil() map[string]string {
	if m.Annotations == nil {
		return map[string]string{}
	}
	return m.Annotations
}

func (o BaseObject) ResourceID() flux.ResourceID {
	ns := o.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	return flux.MakeResourceID(ns, o.Kind, o.Metadata.Name)
}

// It's useful for comparisons in tests to be able to remove the
// record of bytes
func (o *BaseObject) debyte() {
	o.bytes = nil
}

func (o BaseObject) Policy() policy.Set {
	set := policy.Set{}
	for k, v := range o.Metadata.Annotations {
		if strings.HasPrefix(k, PolicyPrefix) {
			p := strings.TrimPrefix(k, PolicyPrefix)
			if v == "true" {
				set = set.Add(policy.Policy(p))
			} else {
				set = set.Set(policy.Policy(p), v)
			}
		}
	}
	return set
}

func (o BaseObject) Source() string {
	return o.source
}

func (o BaseObject) Bytes() []byte {
	return o.bytes
}

func unmarshalObject(source string, bytes []byte) (*BaseObject, error) {
	var base = BaseObject{source: source, bytes: bytes}
	if err := yaml.Unmarshal(bytes, &base); err != nil {
		return nil, err
	}

	// if base.Kind == "List" {
	// 	// This check probably needs to happen in ParseMultidoc.
	// 	// Maybe append it to the map of resources there?
	// 	// IDEA: pass in the map to this function and append via side effect.
	// 	// Loop over list.Items to append.
	// }

	return &base, nil
	// r, err := unmarshalKind(base, bytes)
	// if err != nil {
	// 	return nil, makeUnmarshalObjectErr(source, err)
	// }
	// return r, nil
}

func unmarshalKind(base BaseObject) (resource.Resource, error) {
	bytes := base.bytes
	switch base.Kind {
	case "CronJob":
		var cj = CronJob{BaseObject: base}
		if err := yaml.Unmarshal(bytes, &cj); err != nil {
			return nil, err
		}
		return &cj, nil
	case "DaemonSet":
		var ds = DaemonSet{BaseObject: base}
		if err := yaml.Unmarshal(bytes, &ds); err != nil {
			return nil, err
		}
		return &ds, nil
	case "Deployment":
		var dep = Deployment{BaseObject: base}
		if err := yaml.Unmarshal(bytes, &dep); err != nil {
			return nil, err
		}
		return &dep, nil
	case "Namespace":
		var ns = Namespace{BaseObject: base}
		if err := yaml.Unmarshal(bytes, &ns); err != nil {
			return nil, err
		}
		return &ns, nil
	case "StatefulSet":
		var ss = StatefulSet{BaseObject: base}
		if err := yaml.Unmarshal(bytes, &ss); err != nil {
			return nil, err
		}
		return &ss, nil
	case "":
		// If there is an empty resource (due to eg an introduced comment),
		// we are returning nil for the resource and nil for an error
		// (as not really an error). We are not, at least at the moment,
		// reporting an error for invalid non-resource yamls on the
		// assumption it is unlikely to happen.
		return nil, nil
	// The remainder are things we have to care about, but not
	// treat specially
	default:
		return &base, nil
	}
}

func unmarshalList(source string, base *BaseObject, collection map[string]resource.Resource) error {
	list := List{}
	err := yaml.Unmarshal(base.Bytes(), &list)

	if err != nil {
		return err
	}

	for _, i := range list.Items {
		i.source = source
		r, err := unmarshalKind(i)

		if r == nil {
			continue
		}

		if err != nil {
			return makeUnmarshalObjectErr(source, err)
		}

		collection[r.ResourceID().String()] = r
	}

	return nil
}

func makeUnmarshalObjectErr(source string, err error) *fluxerr.Error {
	return &fluxerr.Error{
		Type: fluxerr.User,
		Err:  err,
		Help: `Could not parse "` + source + `".

This likely means it is malformed YAML.
`,
	}
}

// For reference, the Kubernetes v1 types are in:
// https://github.com/kubernetes/client-go/blob/master/pkg/api/v1/types.go
