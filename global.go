package birch

import (
	"bytes"
	"context"
	"sync"
)

// this global.go file contains shared resources used in the implementation
// of aspects of the package

var iterCtx = context.Background()
var bufPool = &sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}

func getBuf(size int) *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	if size > 0 {
		buf.Grow(size)
	}
	return buf
}

const maxBufPoolSize = 64 << 10
