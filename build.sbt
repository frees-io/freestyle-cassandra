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
  .dependsOn(core, macros)
  .aggregate(core, macros)

lazy val core = project
  .in(file("core"))
  .settings(moduleName := "frees-cassandra-core")
  .settings(scalaMetaSettings)
  .settings(resolvers += Resolver.bintrayRepo("tabdulradi", "maven"))
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)
  .settings(addCompilerPlugin("org.scalameta" % "paradise" % "3.0.0-M10" cross CrossVersion.full))

lazy val macros = project
  .in(file("macros"))
  .settings(moduleName := "frees-cassandra-macros")
  .settings(scalaMetaSettings)
  .settings(libraryDependencies ++= testDependencies)
  .settings(addCompilerPlugin("org.scalameta" % "paradise" % "3.0.0-M10" cross CrossVersion.full))
  .dependsOn(core)

lazy val `macros-tests` = project
  .in(file("macros-tests"))
  .settings(moduleName := "frees-cassandra-macros-tests")
  .settings(scalaMetaSettings)
  .settings(libraryDependencies ++= testDependencies)
  .settings(addCompilerPlugin("org.scalameta" % "paradise" % "3.0.0-M10" cross CrossVersion.full))
  .dependsOn(core, macros)