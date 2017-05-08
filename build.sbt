lazy val root = project
  .in(file("."))
  .settings(name := "freestyle-cassandra")
  .settings(libraryDependencies ++= Seq(
    %%("cats-core"),
    "com.47deg"              %% "freestyle-async" % "0.1.0",
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0",
    "com.datastax.cassandra" % "cassandra-driver-mapping" % "3.2.0",
    "com.datastax.cassandra" % "cassandra-driver-extras" % "3.2.0",
    %%("scalatest")          % "test",
    %%("scalamockScalatest") % "test",
    %%("scalacheck")          % "test"
  ))
