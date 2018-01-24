package resource

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/weaveworks/flux/cluster/kubernetes/testfiles"
	"github.com/weaveworks/flux/resource"
)

// for convenience
func base(source, kind, namespace, name string) BaseObject {
	b := BaseObject{source: source, Kind: kind}
	b.Metadata.Namespace = namespace
	b.Metadata.Name = name
	return b
}

func TestParseEmpty(t *testing.T) {
	doc := ``

	objs, err := ParseMultidoc([]byte(doc), "test")
	if err != nil {
		t.Error(err)
	}
	if len(objs) != 0 {
		t.Errorf("expected empty set; got %#v", objs)
	}
}

func TestParseSome(t *testing.T) {
	docs := `---
kind: Deployment
metadata:
  name: b-deployment
  namespace: b-namespace
---
kind: Deployment
metadata:
  name: a-deployment
`
	objs, err := ParseMultidoc([]byte(docs), "test")
	if err != nil {
		t.Error(err)
	}

	objA := base("test", "Deployment", "", "a-deployment")
	objB := base("test", "Deployment", "b-namespace", "b-deployment")
	expected := map[string]resource.Resource{
		objA.ResourceID().String(): &Deployment{BaseObject: objA},
		objB.ResourceID().String(): &Deployment{BaseObject: objB},
	}

	for id, obj := range expected {
		// Remove the bytes, so we can compare
		if !reflect.DeepEqual(obj, debyte(objs[id])) {
			t.Errorf("At %+v expected:\n%#v\ngot:\n%#v", id, obj, objs[id])
		}
	}
}

func TestParseSomeWithComment(t *testing.T) {
	docs := `# some random comment
---
kind: Deployment
metadata:
  name: b-deployment
  namespace: b-namespace
---
kind: Deployment
metadata:
  name: a-deployment
`
	objs, err := ParseMultidoc([]byte(docs), "test")
	if err != nil {
		t.Error(err)
	}

	objA := base("test", "Deployment", "", "a-deployment")
	objB := base("test", "Deployment", "b-namespace", "b-deployment")
	expected := map[string]resource.Resource{
		objA.ResourceID().String(): &Deployment{BaseObject: objA},
		objB.ResourceID().String(): &Deployment{BaseObject: objB},
	}
	expectedL := len(expected)

	if len(objs) != expectedL {
		t.Errorf("expected %d objects from yaml source\n%s\n, got result: %d\n %#v", expectedL, docs, len(objs), objs)
	}

	for id, obj := range expected {
		// Remove the bytes, so we can compare
		if !reflect.DeepEqual(obj, debyte(objs[id])) {
			t.Errorf("At %+v expected:\n%#v\ngot:\n%#v", id, obj, objs[id])
		}
	}
}

func TestParseSomeLong(t *testing.T) {
	doc := `---
kind: ConfigMap
metadata:
  name: bigmap
data:
  bigdata: |
`
	buffer := bytes.NewBufferString(doc)
	line := "    The quick brown fox jumps over the lazy dog.\n"
	for buffer.Len()+len(line) < 1024*1024 {
		buffer.WriteString(line)
	}

	_, err := ParseMultidoc(buffer.Bytes(), "test")
	if err != nil {
		t.Error(err)
	}
}

func TestParseList(t *testing.T) {
	doc := `---
kind: List
items:
  - kind: Deployment
    metadata:
      name: a-deployment
  - kind: Deployment
    metadata:
      name: b-deployment
`
	src := "my-source"

	objA := base(src, "Deployment", "", "a-deployment")
	objB := base(src, "Deployment", "", "b-deployment")
	expected := map[string]resource.Resource{
		objA.ResourceID().String(): &Deployment{BaseObject: objA},
		objB.ResourceID().String(): &Deployment{BaseObject: objB},
	}

	buffer := bytes.NewBufferString(doc)

	objs, err := ParseMultidoc(buffer.Bytes(), src)

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(expected, objs) {
		t.Errorf("Expected:\n%#s\ngot:\n%#s", expected, objs)
	}

}

func TestParseMultipleLists(t *testing.T) {}

func debyte(r resource.Resource) resource.Resource {
	if res, ok := r.(interface {
		debyte()
	}); ok {
		res.debyte()
	}
	return r
}

func TestLoadSome(t *testing.T) {
	dir, cleanup := testfiles.TempDir(t)
	defer cleanup()
	if err := testfiles.WriteTestFiles(dir); err != nil {
		t.Fatal(err)
	}
	objs, err := Load(dir)
	if err != nil {
		t.Error(err)
	}
	// assume it's one per file for the minute
	if len(objs) != len(testfiles.Files) {
		t.Errorf("expected %d objects from %d files, got result:\n%#v", len(testfiles.Files), len(testfiles.Files), objs)
	}
}
