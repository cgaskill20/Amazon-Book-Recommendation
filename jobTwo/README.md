# Job Two

This job is what will help create the relation between two different titles when they are reviewed by the same person. 
We take as input the User ID, and the names of titles that they left a positive review for. By iterating over these Titles
and creating a bigram connecting two different titles when a person has positively reviewed both, we create the start of a 
link where we can use the connection to help recommend books given a different user has reviewed one of the bigram's books.

_The output of this job will be used in the next job, which is where the actual recommendations will come from._

Commands used to run this job:<br> 
<br>
`$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main *.java`

`jar cf BookBigramsMapReduce.jar *.class`

`$HADOOP_HOME/bin/hadoop jar ~/Amazon-Book-Reccomendation/jobTwo/BookBigramsMapReduce.jar BookBigramsMapReduce -D mapreduce.framework.name=yarn /jobOneoutput /jobTwoOutput`

(You may need to adjust the path to jar accordingly, along with the correct path to the input.)
