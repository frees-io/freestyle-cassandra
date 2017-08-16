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
  "io.github.cassandra-scala" %% "troy-schema" % "0.4.0"
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
  .settings(name := "freestyle-cassandra")
  .settings(noPublishSettings)
  .dependsOn(core)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(moduleName := "freestyle-cassandra-core")
  .settings(scalaMetaSettings)
  .settings(resolvers += Resolver.bintrayRepo("tabdulradi", "maven"))
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)