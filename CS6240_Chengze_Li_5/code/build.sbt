name := "SparkDataMining"

version := "0.1"

scalaVersion := "2.11.11"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "com.github.scala-incubator.io" % "scala-io-file_2.10" % "0.4.3"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.8.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.8.2"
libraryDependencies += "com.github.jai-imageio" % "jai-imageio-core" % "1.3.1"
// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.230"


mainClass in assembly := Some("Main")
assemblyMergeStrategy in assembly := {
  case PathList("com",   "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com",   "squareup", xs @ _*) => MergeStrategy.last
  case PathList("com",   "sun", xs @ _*) => MergeStrategy.last
  case PathList("com",   "thoughtworks", xs @ _*) => MergeStrategy.last
  case PathList("commons-beanutils", xs @ _*) => MergeStrategy.last
  case PathList("commons-cli", xs @ _*) => MergeStrategy.last
  case PathList("commons-collections", xs @ _*) => MergeStrategy.last
  case PathList("commons-io", xs @ _*) => MergeStrategy.last
  case PathList("io",    "netty", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  case PathList("org",   "apache", xs @ _*) => MergeStrategy.last
  case PathList("org",   "codehaus", xs @ _*) => MergeStrategy.last
  case PathList("org",   "fusesource", xs @ _*) => MergeStrategy.last
  case PathList("org",   "mortbay", xs @ _*) => MergeStrategy.last
  case PathList("org",   "tukaani", xs @ _*) => MergeStrategy.last
  case PathList("xerces", xs @ _*) => MergeStrategy.last
  case PathList("xmlenc", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "mydef.key" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}