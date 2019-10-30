package model

import "github.com/mongodb/ftdc/bsonx"

type Command struct {
	DB                 string
	Command            string
	Arguments          *bsonx.Document
	Metadata           *bsonx.Document
	Inputs             []bsonx.Document
	ConvertedFromQuery bool
}

type Delete struct {
	Namespace string
	Filter    *bsonx.Document
}

type Insert struct {
	Namespace string
	Documents []*bsonx.Document
}

type GetMore struct {
	Namespace string
	CursorID  int64
	NReturn   int32
}

type Query struct {
	Namespace string
	Skip      int32
	NReturn   int32
	Query     *bsonx.Document
	Project   *bsonx.Document
}

type Update struct {
	Namespace string
	Filter    *bsonx.Document
	Update    *bsonx.Document

	Upsert bool
	Multi  bool
}

type Reply struct {
	Contents       []*bsonx.Document
	CursorID       int64
	StartingFrom   int32
	CursorNotFound bool
	QueryFailure   bool
}

type Message struct {
	Database   string
	Collection string
	Operation  string
	MoreToCome bool
	Checksum   bool
	Items      []SequenceItem
}

type SequenceItem struct {
	Identifier string
	Documents  []*bsonx.Document
}
