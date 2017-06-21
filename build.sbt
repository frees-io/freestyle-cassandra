pgpPassphrase := Some(getEnvVar("PGP_PASSPHRASE").getOrElse("").toCharArray)
pgpPublicRing := file(s"$gpgFolder/pubring.gpg")
pgpSecretRing := file(s"$gpgFolder/secring.gpg")

lazy val commonDependencies: Seq[ModuleID] = Seq(
  %%("cats-core"),
  %%("freestyle-async"),
  %("cassandra-driver-core"),
  %("cassandra-driver-mapping"),
  %("cassandra-driver-extras"))

lazy val testDependencies: Seq[ModuleID] = Seq(
  %%("scalatest") % "test",
  %%("scalamockScalatest") % "test",
  %%("scalacheck") % "test")

lazy val root = project
  .in(file("."))
  .settings(name := "freestyle-cassandra")
  .settings(noPublishSettings)
  .dependsOn(core)
  .aggregate(core)

lazy val core = project.in(file("core"))
  .settings(moduleName := "freestyle-cassandra-core")
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)