
[comment]: # (Start Badges)

[![Build Status](https://travis-ci.org/frees-io/freestyle-cassandra.svg?branch=master)](https://travis-ci.org/frees-io/freestyle-cassandra) [![codecov.io](http://codecov.io/github/frees-io/freestyle-cassandra/coverage.svg?branch=master)](http://codecov.io/github/frees-io/freestyle-cassandra?branch=master) [![Maven Central](https://img.shields.io/badge/maven%20central-0.0.4-green.svg)](https://oss.sonatype.org/#nexus-search;gav~io.frees~freestyle-cassandra*) [![Latest version](https://img.shields.io/badge/freestyle--cassandra-0.0.4-green.svg)](https://index.scala-lang.org/frees-io/freestyle-cassandra) [![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://raw.githubusercontent.com/frees-io/freestyle-cassandra/master/LICENSE) [![Join the chat at https://gitter.im/47deg/freestyle](https://badges.gitter.im/47deg/freestyle.svg)](https://gitter.im/47deg/freestyle?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![GitHub Issues](https://img.shields.io/github/issues/frees-io/freestyle-cassandra.svg)](https://github.com/frees-io/freestyle-cassandra/issues)

[comment]: # (End Badges)
# Freestyle Cassandra

[Cassandra] atop **Freestyle** is **`frees-cassandra`**.
Freestyle Cassandra is Scala Purely Functional driver for Cassandra based on the datastax Java Driver.

## What’s frees-cassandra

[frees-cassandra] is a library to interact with cassandra built atop Free and using the Datastax 
Cassandra Driver for connecting to a Cassandra instance. It follows the [Freestyle] philosophy, 
being macro-powered. 

## Installation

Add the following resolver and library dependency to your project's build file. 

For Scala `2.11.x` and `2.12.x`:

[comment]: # (Start Replace)
```scala
Resolver.bintrayRepo("tabdulradi", "maven")
libraryDependencies += "io.frees" %% "frees-cassandra" % "0.0.4" 
``` 

[comment]: # (End Replace)

## Documentation

Access to the documentation [here](http://frees.io/docs/cassandra)

[comment]: # (Start Copyright)
# Copyright

Freestyle is designed and developed by 47 Degrees

Copyright (C) 2017 47 Degrees. <http://47deg.com>

[comment]: # (End Copyright)