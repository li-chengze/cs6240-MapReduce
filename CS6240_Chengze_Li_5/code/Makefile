# Makefile for Spark Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone:
	spark-submit --class Main target/scala-2.11/SparkDataMining-assembly-0.1.jar output 100000

