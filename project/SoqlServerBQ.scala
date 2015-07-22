import sbt._
import Keys._

import Dependencies._

object SoqlServerBQ {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly=true) ++ Seq(
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map CommonBQ.genVersion,
    libraryDependencies ++= libraries()
  )

  def libraries() = Seq(
    secondarylib,
    socrataHttpCuratorBroker,
    metricsJetty,
    metricsGraphite
  )
}


