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
  .dependsOn(core, macros, macroTests)
  .aggregate(core, macros, macroTests)

lazy val core = project.in(file("core"))
  .settings(moduleName := "core")
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)

lazy val macros = project.in(file("macros"))
  .settings(moduleName := "macros")
  .settings(libraryDependencies += %("scala-reflect", scalaVersion.value))
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)
  .dependsOn(core)

lazy val macroTests = project.in(file("macro-tests"))
  .settings(moduleName := "macro-tests")
  .settings(libraryDependencies ++= testDependencies)
  .dependsOn(core, macros)