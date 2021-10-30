package file

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	f, err := Open("../testdata/region/r.1.0.mca")
	fmt.Printf("%#v %s\n", f, err)
	fmt.Printf("%v\n", f.header)
	fmt.Println(f.Read(0, 0))
	t.Fail()
}
