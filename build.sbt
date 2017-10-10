pgpPassphrase := Some(getEnvVar("PGP_PASSPHRASE").getOrElse("").toCharArray)
pgpPublicRing := file(s"$gpgFolder/pubring.gpg")
pgpSecretRing := file(s"$gpgFolder/secring.gpg")

lazy val commonDependencies: Seq[ModuleID] = Seq(
  %%("freestyle-async"),
  %%("shapeless"),
  %%("classy-core"),
  %%("classy-config-typesafe"),
  %("cassandra-driver-core"),
  %("cassandra-driver-mapping"),
  %("cassandra-driver-extras"),
  "io.github.cassandra-scala" %% "troy-schema" % "0.4.0",
  "com.propensive"            %% "contextual"  % "1.0.1"
)

lazy val testDependencies: Seq[ModuleID] =
  Seq(%%("scalatest"), %%("scalamockScalatest"), %%("scalacheck"), %%("scheckShapeless")) map (_ % "test")

lazy val root = project
  .in(file("."))
  .settings(name := "freestyle-cassandra")
  .settings(noPublishSettings)
  .dependsOn(core, `macros-tests`)
  .aggregate(core, `macros-tests`)

lazy val core = project
  .in(file("core"))
  .settings(moduleName := "frees-cassandra-core")
  .settings(scalaMetaSettings)
  .settings(resolvers += Resolver.bintrayRepo("tabdulradi", "maven"))
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)

lazy val `macros-tests` = project
  .in(file("macros-tests"))
  .settings(moduleName := "frees-cassandra-macros-tests")
  .settings(scalaMetaSettings)
  .settings(libraryDependencies ++= testDependencies)
  .settings(libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.11.0" % "test")
  .settings(fork in Test in ThisBuild := true)
  .dependsOn(core)
