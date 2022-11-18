# Job Three

This job takes the output of job two and creates a sorted list of recomendations for each book

Commands used to run this job:<br> 
<br>
`$HADOOP_HOME/bin/hadoop fs -rm -r /BookRecomendation3`

`$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main *.java`

`jar cf RecomendationListMapReduce.jar *.class`

`$HADOOP_HOME/bin/hadoop fs -put ~/BookRecomendation3 /`

`$HADOOP_HOME/bin/hadoop jar ~/BookRecomendation3/RecomendationListMapReduce.jar RecomendationListMapReduce /BookRecomendation3/input/ /BookRecomendation3/output`
