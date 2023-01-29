package birch

import (
	"bytes"
	"context"
	"runtime"
	"sync"
)

// this global.go file contains shared resources used in the implementation
// of aspects of the package

var iterCtx = context.Background()
var bufPool = &sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}
var bsPool = &sync.Pool{
	New: func() any { return []byte{} },
}

func getBuf(size int) *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	if size > 0 {
		buf.Grow(size)
	}
	return buf
}

const maxBufPoolSize = 64 << 10

func putBuf(buf *bytes.Buffer) {
	if buf.Len() > maxBufPoolSize {
		buf.Truncate(maxBufPoolSize)
	}
	buf.Reset()
	bufPool.Put(buf)
}

func getMagicBuf(size int) *bytes.Buffer {
	buf := getBuf(size)
	runtime.SetFinalizer(buf, putBuf)
	buf.Grow(size)
	return buf
}
