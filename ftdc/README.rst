======================================================
``ftdc`` -- Golang FTDC Parsing and Generating Library
======================================================

Overview
--------

FTDC, originally short for *full time diagnostic data capture*, is MongoDB's
internal diagnostic data collection facility. It encodes data in a
space-efficient format, which allows MongoDB to record diagnostic information
every second, and store weeks of data with only a few hundred megabytes of
storage.

The FTDC data format is based on BSON, and provides ways to store series of
documents with the same schema/structure in a compressed columnar format.

This library provides a fully-featured and easy to use toolkit for
interacting data stored in this format in Go programs. The library
itself originated as a `project by 2016 Summer interns at MongoDB
<https://github.com/10gen/ftdc-utils>`_ but has diverged substantially
since then, and adds features for generating data in this format.

Use
---

All documentation is in the `godoc
<https://godoc.org/github.com/deciduosity/ftdc>`_.

This library is available for use under the terms of Apache License.

Features
--------

This library supports parsing of the FTDC data format and
several ways of iterating these results. Additionally, it provides the
ability to create FTDC payloads, and is the only extant (?) tool for
generating FTDC data outside of the MongoDB code base.

The library includes tools for generating FTDC payloads and document
streams as well as iterators and tools for accessing data from FTDC
files. All functionality is part of the ``ftdc`` package, and the API
is fully documented.

The ``events`` and ``metrics`` sub-packages provide higher level functionality
for collecting data from performance tests (events), and more generalized
system metrics collection (metrics).

Development
-----------

This project emerged from work at MongoDB to support this fork drops support
older versions of Golang, thereby adding support for modules.

Pull requests are welcome. Feel free to create issues with enhancements or
bugs.
