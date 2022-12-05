name := "spark-homework"

version := "0.1"

scalaVersion := "2.13.10"

idePackagePrefix := Some("lev.pyryanov")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"

