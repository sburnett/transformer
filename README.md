transformer
===========

A small Go framework for processing on data in LevelDB. It is meant for quickly
analyzing medium-sized data on a single multi-core machine. I define
"medium-sized" to be anything between several gigabytes up to one or two
terabytes.

For simple computations, transformer is usually I/O bound. On not-so-simple
computations, it uses goroutines to parallelize computation across multiple
cores in the machine when appropriate.

See http://godoc.org/github.com/sburnett/transformer for documentation.

Examples written using the framework:
- https://github.com/sburnett/transformer-diagnostics
- https://github.com/sburnett/bismark-passive-server-go
- https://github.com/sburnett/bismark-tools

[![Build Status](https://travis-ci.org/sburnett/transformer.png)](https://travis-ci.org/sburnett/transformer)
