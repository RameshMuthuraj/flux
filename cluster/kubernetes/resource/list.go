package resource

type listOfObjects []baseObject

type List struct {
	Kind  string       `yaml:"kind"`
	Items []baseObject `yaml:"items"`
}
