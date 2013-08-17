import com.typesafe.startscript.StartScriptPlugin

seq(StartScriptPlugin.startScriptForClassesSettings: _*)

name := "oomupdater"

version := "1.0"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.0.13",
	"mysql" % "mysql-connector-java" % "5.1.6"
)
