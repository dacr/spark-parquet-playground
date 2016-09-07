name := "spark-parquet-playground"

version := "0.1"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation" , "-feature", "-language:implicitConversions")

mainClass in assembly := Some("dummy.Dummy")

jarName in assembly := "dummy.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql"  % "2.0.0",
  "org.anarres.lzo"  %  "lzo-hadoop" % "1.0.5",
  //"com.twitter"      %  "parquet"    % "2.1.0"
  //"org.apache.parquet" % "parquet-format" % "2.3.1"
  "com.twitter" %% "scalding-parquet" % "0.16.1-RC3"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

libraryDependencies += "junit" % "junit" % "4.12" % "test"

initialCommands in console := """import dummy._"""

sourceGenerators in Compile <+= 
 (sourceManaged in Compile, version, name, jarName in assembly) map {
  (dir, version, projectname, jarexe) =>
  val file = dir / "dummy" / "MetaInfo.scala"
  IO.write(file,
  """package dummy
    |object MetaInfo { 
    |  val version="%s"
    |  val project="%s"
    |  val jarbasename="%s"
    |}
    |""".stripMargin.format(version, projectname, jarexe.split("[.]").head) )
  Seq(file)
}

resolvers += "conjars" at "http://conjars.org/repo"  // required by some scalding-parquet dependencies !
