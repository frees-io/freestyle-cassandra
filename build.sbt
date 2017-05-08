pgpPassphrase := Some(getEnvVar("PGP_PASSPHRASE").getOrElse("").toCharArray)
pgpPublicRing := file(s"$gpgFolder/pubring.gpg")
pgpSecretRing := file(s"$gpgFolder/secring.gpg")

lazy val root = project.in(file("."))
  .settings(name := "freestyle-cassandra")
  .settings(libraryDependencies ++= Seq(
    %%("cats-core"),
    %%("freestyle", "0.1.0"),
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0",
    "com.datastax.cassandra" % "cassandra-driver-mapping" % "3.2.0",
    "com.datastax.cassandra" % "cassandra-driver-extras" % "3.2.0",
    %%("scalatest")          % "test"))