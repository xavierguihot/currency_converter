name := "currency_converter"

version := "1.0.4"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xfatal-warnings")

assemblyJarName in assembly := name.value + "-" + version.value + ".jar"

assemblyOutputPath in assembly := file("./" + name.value + "-" + version.value + ".jar")

val sparkVersion        = "2.1.0"
val jodaTimeVersion     = "2.9.9"
val jodaConvertVersion  = "1.9.2"
val scalaTestVersion    = "3.0.1"
val sparkTestVersion    = "2.1.0_0.8.0"

libraryDependencies ++= Seq(
	"org.apache.spark"   %% "spark-core"         % sparkVersion        % "provided",
	"joda-time"          %  "joda-time"          % jodaTimeVersion,
	"org.joda"           %  "joda-convert"       % jodaConvertVersion,
	"org.scalatest"      %% "scalatest"          % scalaTestVersion    % "test",
	"com.holdenkarau"    %% "spark-testing-base" % sparkTestVersion    % "test"
)

parallelExecution in Test := false
