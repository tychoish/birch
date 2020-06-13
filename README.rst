==============================================
``birch`` -- Go Lang BSON Manipulation Package
==============================================

Overview
--------

The ``birch`` package provides an API for manipulating bson in Go programs
without needing to handle byte slices or maintain types for marshalling bson
into using reflection. It provides type safety and an ergonomic interface for
building and manipulating BSON documents.

This code is an evolution from an earlier phase of development of the official
`MongoDB Go Driver's BSON library <https://godoc.org/go.mongodb.org/mongo-driver/bson>`_,
but both libraries have diverged. For most application uses the official BSON
library is a better choice for interacting with BSON in most circumstances.

The Document type in this library implements bson library's Marhsaler and
Unmarshaller interfaces, to support improved interoperation, and provides the
additional DocumentMarshaler to allow types that are directly convertable to
Documents, without needing to round trip through a serialized format.

API and Documentation
---------------------

See the `godoc API documentation
<http://godoc.org/github.com/deciduosity/birch>`_ for more information
about amboy interfaces and internals.

Development
-----------

Birch is available under the terms of the Apache License.

While the library is largely feature complete, if you encounter problems, feel
free to open pull requests or create issues reporting any issues that you see.
