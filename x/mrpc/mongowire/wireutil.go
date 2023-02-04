package mongowire

import (
	"errors"
	"io"

	"github.com/tychoish/birch"
	"github.com/tychoish/fun"
)

func readInt32(b []byte) int32 {
	return (int32(b[0])) |
		(int32(b[1]) << 8) |
		(int32(b[2]) << 16) |
		(int32(b[3]) << 24)
}

func readInt64(b []byte) int64 {
	return (int64(b[0])) |
		(int64(b[1]) << 8) |
		(int64(b[2]) << 16) |
		(int64(b[3]) << 24) |
		(int64(b[4]) << 32) |
		(int64(b[5]) << 40) |
		(int64(b[6]) << 48) |
		(int64(b[7]) << 56)
}

func writeInt32(i int32, wr io.Writer) int {
	return int(fun.Must(wr.Write(encodeInt32(i))))
}

func encodeInt32(i int32) []byte {
	buf := make([]byte, 4)
	buf[0] = byte(i)
	buf[1] = byte(i >> 8)
	buf[2] = byte(i >> 16)
	buf[3] = byte(i >> 24)
	return buf
}

func writeInt64(i int64, wr io.Writer) int {
	return int(fun.Must(wr.Write(encodeInt64(i))))
}

func encodeInt64(i int64) []byte {
	buf := make([]byte, 8)
	buf[0] = byte(i)
	buf[1] = byte(i >> 8)
	buf[2] = byte(i >> 16)
	buf[3] = byte(i >> 24)
	buf[4] = byte(i >> 32)
	buf[5] = byte(i >> 40)
	buf[6] = byte(i >> 48)
	buf[7] = byte(i >> 56)
	return buf
}

func readCString(b []byte) (string, error) {
	for i := 0; i < len(b); i++ {
		if b[i] == '\x00' {
			return string(b[0:i]), nil
		}
	}

	return "", errors.New("c string with no terminator")
}

func writeCString(s string, wr io.Writer) int {
	wr.Write([]byte(s))
	wr.Write([]byte{0})
	return len(s) + 1
}

func getDocSize(doc *birch.Document) int {
	if doc == nil {
		return 0
	}
	return int(fun.Must(doc.Validate()))
}
