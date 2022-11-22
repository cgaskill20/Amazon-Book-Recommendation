$HADOOP_HOME/bin/hadoop fs -mkdir /dataSplit
$HADOOP_HOME/bin/hadoop fs -rm -r /dataSplit/*
sbt package
spark-submit --class Split --deploy-mode cluster --supervise target/scala-2.12/splitscala_2.12-0.1.jar /jobOneoutput /dataSplit/train /dataSplit/test