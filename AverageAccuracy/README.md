# Average Accuracy

This is a simple job that takes the output of the accuracy job, and finds the average of all the accuracies.

Commands used to run this job:<br> 
<br>
`$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main *.java`

`jar cf AverageAccuracy.jar *.class`

`$HADOOP_HOME/bin/hadoop jar ~/Amazon-Book-Recommendation/AverageAccuracy/AverageAccuracy.jar AverageAccuracyMapReduce -D mapreduce.framework.name=yarn /averages /AvgAcc`

(You may need to adjust the path to jar accordingly, along with the correct path to the input.)
