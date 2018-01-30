# Makefile for Spark Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------
jar.name=WeatherReportWithCombiner-1.0-SNAPSHOT.jar
jar.path=target/scala-2.11/SparkPageRank-assembly-0.1.jar
local.input=input
local.output=output

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone:
	spark-submit ${jar.path} ${local.input} ${local.output}
