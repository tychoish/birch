==================================================================
``birch`` -- Flexible Document Encoding and Manipulation Framework
==================================================================

Overview
--------

``birch``, is a Go library for manipulating "documents." Documents are
collections of key-value types with richly typed values, and in the same sense
as the documents at the core of MongoDB's data model. Indeed, it is descended
from an early version of `the BSON library for the current MongoDB Go Driver
<https://github.com/mongodb/mongo-go-driver/>`_; however, the goals of
``birch`` have diverged. ``birch`` exists to provide an API for creating and
manipulating documents without using reflection or the MongoDB
driver, without loosing type fidelity, while still producing valid BSON and
JSON representations of a document.

In addition to document production, the birch package contains two other
additional areas of functionality that build upon the core document
manipulation layer:

- ``ftdc`` provides a document-based, columnar, format ideal for storing
  timeseries data. This package is useful for storing high resolution system
  or application metrics data. The format is highly compressed and the tools
  can support high volume data collection tools. This is the same format that
  the MongoDB service produces for its internal diagnostic data capture, and
  this package provides an API for reading and writing data in this format, as
  well as tools in support of creating metrics for this format.

- ``mrpc`` provides a set of low level tools for interacting with
  ""mongodb-wire-protocol" RPC services. This might be useful for writing
  fairly low-level interactions with MongoDB services, you might also be able
  to use this as the basis for providing the *server* half that is compatible
  with most MongoDB protocol implementations.

In most cases if you have Go objects that (structs, etc) that you want to
convert to BSON you should use the Go driver's bson library. However, if you
need to programatically interact with BSON and want a reasonable, type-safe,
API for that, birch is may be the right thing!

API and Documentation
---------------------

See `the godoc <https://pkg.go.dev/github.com/deciduosity/birch>`_ for full
API documentation.

Development
-----------

The API is stable, and the library is largely feature complete. The following
areas may see some additional development:

- adding additional higher-level tools for interacting with
  timeseries data, including new collectors and first-class support for
  additional data formats.

- additional document and element constructors for new types and
  type-combinations, to improve the behavior of the ``birch.EC.Interface()``
  constructor.

The existing API wouldn't change to add support for these features.
