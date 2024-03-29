// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"strings"
	"testing"

	"encoding/binary"
	"time"
)

func TestNew(t *testing.T) {
	// Ensure that objectid.New() doesn't panic.
	NewObjectID()
}

func TestString(t *testing.T) {
	id := NewObjectID()
	if !strings.Contains(id.String(), id.Hex()) {
		t.Fatal("string of id should contain hex rep")
	}
}

func TestFromHex_RoundTrip(t *testing.T) {
	before := NewObjectID()
	after, err := ObjectIDFromHex(before.Hex())
	if err != nil {
		t.Fatal(err)
	}

	if before != after {
		t.Fatalf("unqueal %v and %v", before, after)
	}
}

func TestFromHex_InvalidHex(t *testing.T) {
	_, err := ObjectIDFromHex("this is not a valid hex string!")
	if err == nil {
		t.Fatal(err)
	}
}

func TestFromHex_WrongLength(t *testing.T) {
	_, err := ObjectIDFromHex("deadbeef")
	if ErrInvalidHex != err {
		t.Fatalf("unqueal %v and %v", ErrInvalidHex, err)
	}
}

func TestTimeStamp(t *testing.T) {
	testCases := []struct {
		Hex      string
		Expected string
	}{
		{
			"000000001111111111111111",
			"1970-01-01 00:00:00 +0000 UTC",
		},
		{
			"7FFFFFFF1111111111111111",
			"2038-01-19 03:14:07 +0000 UTC",
		},
		{
			"800000001111111111111111",
			"2038-01-19 03:14:08 +0000 UTC",
		},
		{
			"FFFFFFFF1111111111111111",
			"2106-02-07 06:28:15 +0000 UTC",
		},
	}

	for _, testcase := range testCases {
		id, err := ObjectIDFromHex(testcase.Hex)
		if err != nil {
			t.Fatal(err)
		}

		secs := int64(binary.BigEndian.Uint32(id[0:4]))
		timestamp := time.Unix(secs, 0).UTC()
		if testcase.Expected != timestamp.String() {
			t.Fatalf("unqueal %v and %v", testcase.Expected, timestamp.String())
		}
	}
}

func TestCounterOverflow(t *testing.T) {
	objectIDCounter = 0xFFFFFFFF

	_ = NewObjectID()

	if uint32(0) != objectIDCounter {
		t.Fatalf("unqueal %v and %v", uint32(0), objectIDCounter)
	}
}
