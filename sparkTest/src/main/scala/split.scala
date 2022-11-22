import org.apache.spark.sql.SparkSession

object Split {

    def main(args: Array[String]): Unit = {
        //takes job one output file, and divides into two different directories dataSplit/test/* and dataSplit/train/*
	    val sc = SparkSession.builder().master("spark://nashville.cs.colostate.edu:30276").getOrCreate().sparkContext
    	val userMap = sc.textFile(args(0));
        val sets = userMap.randomSplit(Array[Double](.7, .3), 42)
        val train = sets(0)
        val test = sets(1)
        train.saveAsTextFile(args(1))
        test.saveAsTextFile(args(2))
    }
}