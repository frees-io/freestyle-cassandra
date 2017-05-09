lazy val root = project
  .in(file("."))
  .settings(name := "freestyle-cassandra")
  .settings(libraryDependencies ++= Seq(
    %%("cats-core"),
    %%("freestyle-async"),
    %("cassandra-driver-core"),
    %("cassandra-driver-mapping"),
    %("cassandra-driver-extras"),
    %%("scalatest")          % "test",
    %%("scalamockScalatest") % "test",
    %%("scalacheck")          % "test"
  ))
