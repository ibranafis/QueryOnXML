name := "QueryOnXML"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies+="org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies+="com.databricks" % "spark-avro_2.10" % "2.0.1"
libraryDependencies+="com.databricks" %% "spark-xml" % "0.7.0"
libraryDependencies+="mysql" % "mysql-connector-java" % "8.0.18"