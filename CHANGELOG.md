# Changelog

## 11/10/2017 - Version 0.0.4

Release changes:

* Uuse the async traits for implicits from freestyle ([#121](https://github.com/frees-io/freestyle-cassandra/pull/121))
* Restores asResultSet[M[_]] definition ([#120](https://github.com/frees-io/freestyle-cassandra/pull/120))
* Release 0.0.4 ([#122](https://github.com/frees-io/freestyle-cassandra/pull/122))


## 11/08/2017 - Version 0.0.3

Release changes:

* ResultSet API implementations ([#116](https://github.com/frees-io/freestyle-cassandra/pull/116))
* Refactorizes handlers and provides implicits ([#117](https://github.com/frees-io/freestyle-cassandra/pull/117))
* Adds the operations for fetching model from a query ([#118](https://github.com/frees-io/freestyle-cassandra/pull/118))


## 11/06/2017 - Version 0.0.2

Release changes:

* Update CHANGELOG.md ([#87](https://github.com/frees-io/freestyle-cassandra/pull/87))
* Macro for expanding interpolator ([#89](https://github.com/frees-io/freestyle-cassandra/pull/89))
* Uses sbt-embedded-cassandra and introduce the Integration Tests ([#90](https://github.com/frees-io/freestyle-cassandra/pull/90))
* Removes the ScalaJS badge from README ([#97](https://github.com/frees-io/freestyle-cassandra/pull/97))
* Allows setting the consistency level at query level ([#96](https://github.com/frees-io/freestyle-cassandra/pull/96))
* Adds the code for creating case classes from rows ([#103](https://github.com/frees-io/freestyle-cassandra/pull/103))
* Add printer arbitrary for scalacheck testing ([#108](https://github.com/frees-io/freestyle-cassandra/pull/108))
* Allows the validation of schema definition statements ([#109](https://github.com/frees-io/freestyle-cassandra/pull/109))
* Adds the printer to the FieldLister ([#110](https://github.com/frees-io/freestyle-cassandra/pull/110))
* Use the printer in the FieldMapper ([#111](https://github.com/frees-io/freestyle-cassandra/pull/111))
* Release v0.0.2 ([#112](https://github.com/frees-io/freestyle-cassandra/pull/112))


## 10/09/2017 - Version 0.0.1

Release changes:

* Removes macro implementation ([#13](https://github.com/frees-io/freestyle-cassandra/pull/13))
* Adds a statement generator ([#14](https://github.com/frees-io/freestyle-cassandra/pull/14))
* Keyspace schema parser ([#15](https://github.com/frees-io/freestyle-cassandra/pull/15))
* Adds the table model ([#25](https://github.com/frees-io/freestyle-cassandra/pull/25))
* Adds the cluster configuration for Typesafe Config ([#30](https://github.com/frees-io/freestyle-cassandra/pull/30))
* Adds statement config model and tests ([#31](https://github.com/frees-io/freestyle-cassandra/pull/31))
* Upgrades sbt-freestyle ([#34](https://github.com/frees-io/freestyle-cassandra/pull/34))
* Cluster information ([#35](https://github.com/frees-io/freestyle-cassandra/pull/35))
* Decouples the typesafe config library ([#36](https://github.com/frees-io/freestyle-cassandra/pull/36))
* Cluster Config Decoder tests ([#37](https://github.com/frees-io/freestyle-cassandra/pull/37))
* FieldMapper ([#39](https://github.com/frees-io/freestyle-cassandra/pull/39))
* Schema validator ([#50](https://github.com/frees-io/freestyle-cassandra/pull/50))
* Remote scheme provider ([#53](https://github.com/frees-io/freestyle-cassandra/pull/53))
* Improves test coverage ([#55](https://github.com/frees-io/freestyle-cassandra/pull/55))
* Allows the creation of distinct names in arbitraries ([#56](https://github.com/frees-io/freestyle-cassandra/pull/56))
* Abstract schema provider and validator functions over M[_] ([#54](https://github.com/frees-io/freestyle-cassandra/pull/54))
* Fixes distinct user type name generation ([#64](https://github.com/frees-io/freestyle-cassandra/pull/64))
* Renames to free cassandra ([#66](https://github.com/frees-io/freestyle-cassandra/pull/66))
* Adds the basic interpolator ([#69](https://github.com/frees-io/freestyle-cassandra/pull/69))
* Adds support for params in the interpolator ([#73](https://github.com/frees-io/freestyle-cassandra/pull/73))
* Abstract ByteBufferCodec over M[_] ([#74](https://github.com/frees-io/freestyle-cassandra/pull/74))
* Metadata schema provider from cluster configuration ([#75](https://github.com/frees-io/freestyle-cassandra/pull/75))
* Create StatementAPI ([#76](https://github.com/frees-io/freestyle-cassandra/pull/76))
* Adds new statement operations ([#77](https://github.com/frees-io/freestyle-cassandra/pull/77))
* Fixes the project name for sbt-org-policies ([#78](https://github.com/frees-io/freestyle-cassandra/pull/78))
* Implement asResultSet method for interpolator ([#80](https://github.com/frees-io/freestyle-cassandra/pull/80))
* Execute query without prepare ([#83](https://github.com/frees-io/freestyle-cassandra/pull/83))
* Releases RC1 ([#84](https://github.com/frees-io/freestyle-cassandra/pull/84))
* Returns a FreeS[M, ResultSet] from the interpolator ([#86](https://github.com/frees-io/freestyle-cassandra/pull/86))