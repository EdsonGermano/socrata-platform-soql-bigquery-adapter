import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "soql-postgres-adapter",
    file(".")
  ).settings(BuildSettings.buildSettings: _*)
   .aggregate(commonBQ, storeBQ, soqlServerBQ)
   .dependsOn(commonBQ, storeBQ, soqlServerBQ)

  def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).settings(settings.settings : _*).dependsOn(dependencies: _*)

  lazy val commonBQ = p("common-bq", CommonPG)
  lazy val storeBQ = p("store-bq", StorePG) dependsOn(commonBQ % "test->test;compile->compile")
  lazy val soqlServerBQ = p("soql-server-bq", SoqlServerPG) dependsOn(storeBQ % "test->test;compile->compile")
}
