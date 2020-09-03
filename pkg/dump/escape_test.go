package dump_test

import (
	"testing"

	"github.com/partyzanex/repmy/pkg/dump"
	"github.com/partyzanex/testutils"
)

func TestEscape(t *testing.T) {
	input := []byte{0, '\n', '\r', '\\', '\'', '"', '\032', 'a'}
	expected := []byte(`\0\n\r\\\'\"\Za`)
	result := dump.Escape(input)
	testutils.AssertEqual(t, "escape", string(expected), string(result))
}

func BenchmarkEscape(b *testing.B) {
	input := []byte{0, '\n', '\r', '\\', '\'', '"', '\032', 'a'}

	for i := 0; i < b.N; i++ {
		_ = dump.Escape(input)
	}
}
