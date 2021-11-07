package file

import (
	"testing"

	"github.com/yehan2002/is/v2"
)

type testReader struct{}

func TestReader(t *testing.T) { is.Suite(t, &testReader{}) }

func (t *testReader) TestSize(is is.Is) {
	_, err := NewReader(nil, 10)
	is.Err(err, ErrSize, "files must have at least 2 sections.")
}
