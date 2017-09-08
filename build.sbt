pgpPassphrase := Some(getEnvVar("PGP_PASSPHRASE").getOrElse("").toCharArray)
pgpPublicRing := file(s"$gpgFolder/pubring.gpg")
pgpSecretRing := file(s"$gpgFolder/secring.gpg")

lazy val commonDependencies: Seq[ModuleID] = Seq(
  %%("cats-core"),
  %%("freestyle-async"),
  %%("shapeless"),
  %%("circe-core"),
  %%("circe-parser"),
  %%("circe-generic"),
  %%("classy-core"),
  %("cassandra-driver-core"),
  %("cassandra-driver-mapping"),
  %("cassandra-driver-extras"),
  "io.github.cassandra-scala" %% "troy-schema" % "0.4.0",
  "com.propensive" %% "contextual" % "1.0.1"
)

lazy val testDependencies: Seq[ModuleID] =
  Seq(
    %%("scalatest"),
    %%("scalamockScalatest"),
    %%("scalacheck"),
    %%("scheckShapeless"),
    %%("classy-config-typesafe")) map (_  % "test")

lazy val root = project
  .in(file("."))
  .settings(name := "frees-cassandra")
  .settings(noPublishSettings)
  .dependsOn(core)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(moduleName := "frees-cassandra-core")
  .settings(scalaMetaSettings)
  .settings(resolvers += Resolver.bintrayRepo("tabdulradi", "maven"))
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)

lazy val testMacro = project
  .in(file("testMacro"))
  .settings(moduleName := "freestyle-cassandra-test-macro")
  .settings(scalaMetaSettings)
  .dependsOn(core)

lazy val test = project
  .in(file("test"))
  .settings(moduleName := "freestyle-cassandra-test")
  .settings(scalaMetaSettings)
  .dependsOn(core, testMacro)