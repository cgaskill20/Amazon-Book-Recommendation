# Data Split

This job will allow us to split raw data into a train and a test split where it will then be processed in job one.
We will use a 70/30 split. We read the raw data using Spark, and use the built in
Spark API to do this split. This will allow us to test for accuracy in the following job.


Commands used to run this job:<br> 
<br>
`$HADOOP_HOME/bin/hadoop fs -mkdir /dataSplit`

`$HADOOP_HOME/bin/hadoop fs -rm -r /dataSplit/*`

`sbt package`

`spark-submit --class Split --deploy-mode cluster --supervise target/scala-2.12/splitscala_2.12-0.1.jar /Books_rating.csv /dataSplit/train /dataSplit/test`

These commands are all inside of the `run.sh` file. As long as your current directory is `/dataSplit` then running this shellscript will execute all of
the above commands. (May have to adjust input location)
