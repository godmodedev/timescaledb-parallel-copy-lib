## timescaledb-parallel-copy

This is a fork of [`timescaledb-parallel-copy`](//github.com/timescale/timescaledb-parallel-copy), 
modified to be used in a Pythonic manner with any Python-based data processing workflow.
`timescaledb-parallel-copy-lib` as it exists here is a Python package which wraps the 
original internals from the [`timescaledb-parallel-copy`](//github.com/timescale/timescaledb-parallel-copy)
command line program. It offers an interface for parallelizing
PostgreSQL's built-in `COPY` functionality for bulk inserting data
into [TimescaleDB.](//github.com/timescale/timescaledb/)

### Getting started

TODO

Before using this library to bulk insert data, your database should
be installed with the TimescaleDB extension and the target table
should already be made a hypertable.

### Purpose

PostgreSQL native `COPY` function is transactional and single-threaded, and may not be suitable for ingesting large
amounts of data. Assuming the file is at least loosely chronologically ordered with respect to the hypertable's time
dimension, this library should give you great performance gains by parallelizing this operation, allowing users to take
full advantage of their hardware.

This library also takes care to ingest data in a more efficient manner by roughly preserving the order of the rows. By
taking a "round-robin" approach to sharing inserts between parallel workers, the database has to switch between chunks
less often. This improves memory management and keeps operations on the disk as sequential as possible.


#### Running Tests

Some of the tests require a running Postgres database. Set the `TEST_CONNINFO`
environment variable to point at the database you want to run tests against.
(Assume that the tests may be destructive; in particular it is not advisable to
point the tests at any production database.)

For example:
```
$ createdb gotest
$ TEST_CONNINFO='dbname=gotest user=myuser' go test -v ./...
```
