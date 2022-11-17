# Job One

This job simply maps a user to all the books they have positively reviewed.

Commands used to run this job:<br> 
<br>
`$HADOOP_HOME/bin/hadoop fs -rm -r /BookRecomendation`

`$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main *.java`

`jar cf UserToBooksMapReduce.jar *.class`

`$HADOOP_HOME/bin/hadoop fs -put ~/BookRecomendation /`

`$HADOOP_HOME/bin/hadoop jar ~/BookRecomendation/UserToBooksMapReduce.jar UserToBooksMapReduce /BookRecomendation/input/ /BookRecomendation/output`
