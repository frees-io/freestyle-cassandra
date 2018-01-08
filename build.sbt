import sbtorgpolicies.templates.badges._
import sbtorgpolicies.runnable.syntax._

pgpPassphrase := Some(getEnvVar("PGP_PASSPHRASE").getOrElse("").toCharArray)
pgpPublicRing := file(s"$gpgFolder/pubring.gpg")
pgpSecretRing := file(s"$gpgFolder/secring.gpg")

lazy val freesV = "0.5.1"

lazy val commonDependencies: Seq[ModuleID] = Seq(
  %%("frees-async",freesV),
  %%("frees-async-guava",freesV),
  %%("shapeless"),
  %%("classy-core"),
  %%("classy-config-typesafe"),
  %("cassandra-driver-core"),
  %("cassandra-driver-mapping"),
  %("cassandra-driver-extras"),
  "io.github.cassandra-scala" %% "troy-schema" % "0.5.0",
  "com.propensive"            %% "contextual"  % "1.0.1"
)

lazy val testDependencies: Seq[ModuleID] =
  Seq(%%("scalatest"), %%("scalamockScalatest"), %%("scalacheck"), %%("scheckShapeless"))
    .map(_ % "it,test")

lazy val orgSettings = Seq(
  orgBadgeListSetting := List(
    TravisBadge.apply,
    CodecovBadge.apply,
    MavenCentralBadge.apply,
    ScalaLangBadge.apply,
    LicenseBadge.apply,
    { info => GitterBadge.apply(info.copy(owner = "47deg", repo = "freestyle")) },
    GitHubIssuesBadge.apply
  ),
  embeddedCassandraCQLFileSetting := Option(file("macros-tests/src/main/resources/schema.sql"))
)
orgAfterCISuccessTaskListSetting := List(
    depUpdateDependencyIssues.asRunnableItem,
    orgPublishReleaseTask.asRunnableItem(allModules = true, aggregated = false, crossScalaVersions = true),
    orgUpdateDocFiles.asRunnableItem
     )

lazy val root = project
  .in(file("."))
  .settings(name := "freestyle-cassandra")
  .settings(noPublishSettings)
  .enablePlugins(EmbeddedCassandraPlugin)
  .settings(orgSettings)
  .dependsOn(core, `macros-tests`, docs)
  .aggregate(core, `macros-tests`, docs)

lazy val core = project
  .in(file("core"))
  .settings(moduleName := "frees-cassandra-core")
  .settings(scalaMetaSettings)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(resolvers += Resolver.bintrayRepo("tabdulradi", "maven"))
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= testDependencies)

lazy val `macros-tests` = project
  .in(file("macros-tests"))
  .settings(moduleName := "frees-cassandra-macros-tests")
  .settings(scalaMetaSettings)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(libraryDependencies ++= testDependencies)
  .dependsOn(core)

lazy val docs = project
  .in(file("docs"))
    .dependsOn(core)
    .aggregate(core)
  .settings(name := "frees-cassandra-docs")
  .settings(noPublishSettings: _*)
  .settings(
      addCompilerPlugin(%%("scalameta-paradise") cross CrossVersion.full),
      libraryDependencies += %%("scalameta", "1.8.0"),
      scalacOptions += "-Xplugin-require:macroparadise",
      scalacOptions in Tut ~= (_ filterNot Set("-Ywarn-unused-import", "-Xlint").contains),
      // Pointing to https://github.com/frees-io/freestyle/tree/master/docs/src/main/tut/docs/rpc
          tutTargetDirectory := baseDirectory.value.getParentFile.getParentFile / "docs" / "src"
              / "main" / "tut" / "docs" / "cassandra"
  )
  .enablePlugins(TutPlugin)