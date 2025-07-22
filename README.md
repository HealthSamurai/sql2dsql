# sql2dsq

Library for converting `sql` queiries to [dsql](https://github.com/HealthSamurai/dsql) structures.

## Overview

This library provides a simple way to parse PostgreSQL queries in Clojure applications. It uses the [libpg_query](https://github.com/pganalyze/libpg_query) library under the hood to parse queries into a JSON representation of the PostgreSQL parse tree. To use this library in Java, [JNA](https://github.com/java-native-access/jna) is utilized. You can find the JNA wrapper code in the `java` folder.

## Installation

At the moment, the resources include a prebuilt `libpq_query` library for macOS to facilitate a quick start for developers.

Go to `java` folder and run:
```
mvn clean package
```

After that you can start REPL :)

## Development

All code is located in `src/sql2dsql/core`. The `parse-sql` function returns an AST in EDN format. The multimethod `stmt->dsql` takes this AST and transforms it into dsql structure.
You can find a list of all AST node types [here](https://github.com/pganalyze/libpg_query/blob/17-latest/src/postgres/include/nodes/parsenodes.h). For each node, there is a corresponding stmt->dsql method implementation; however, not all AST nodes are currently supported.

You can play with core functions in `user` namespace:

```clojure
(ns user
  (:require [sql2dsql.core :refer [->dsql parse-sql]]))

(parse-sql "select * from patient where id = '1'")
(->dsql "select * from patient where id = '1'")
```

To run tests:
```bash
clojure -X:test
```

## Requirements

- JDK 11 or later
- Clojure 1.11 or later
- libpg_query native library must be available in your system's library path

## License

Distributed under the Eclipse Public License version 1.0.
