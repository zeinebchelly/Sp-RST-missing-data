name := "qfs"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark"  %% "spark-mllib"  % sparkVersion % "provided"
)
