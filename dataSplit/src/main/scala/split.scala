import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.feature.StringIndexer


object Split {

    def main(args: Array[String]): Unit = {
        //takes job one output file, and divides into two different directories dataSplit/test/* and dataSplit/train/*
	    val sc = SparkSession.builder().master("spark://nashville.cs.colostate.edu:30276").getOrCreate().sparkContext
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._
        val data = sc.textFile(args(0))
        val header = data.first()
        val testing = data.filter(x => x!= header)
        val userMap = testing

        //parses the lines
        val features = userMap.map(s=>(s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(0), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(1), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(2), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(3), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(4), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(5), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(6).toFloat, s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(7), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(8), s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(9)))
        var df = features.toDF("Id","Title","Price","User_id","profileName","review/helpfulness","review/score","review/time","review/summary","review/text")
        
        //adds new columns for unique book and user int ids
        val userIndex = new StringIndexer()
            .setInputCol("User_id")
            .setOutputCol("User_int")
        val withUserInt = userIndex.fit(df)
        df = withUserInt.transform(df)
        val bookIndex = new StringIndexer()
            .setInputCol("Id")
            .setOutputCol("Book_int")
        val withBookInt = bookIndex.fit(df)
        df = withBookInt.transform(df)

        //split into train/test
        val Array(training, test) = df.randomSplit(Array[Double](.7, .3), 42)

        //set up the Alternating Least Squares model
        val als = new ALS()
            .setMaxIter(10)
            .setRegParam(0.01)
            .setUserCol("User_int")
            .setItemCol("Book_int")
            .setRatingCol("review/score")
        val model = als.fit(training)
        model.setColdStartStrategy("drop")
        
        //create the testing predictions
        val predictions = model.transform(test)

        //sets up the evaluation using the review scores. uses root mean squared error
        val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("review/score")
            .setPredictionCol("prediction")
        val rmse = evaluator.evaluate(predictions)
        //creates dataframe which includes top 10 books for user, and top 10 users for book.
        val userRecs = model.recommendForAllUsers(10)
        val movieRecs = model.recommendForAllItems(10)

        //prints in the sparkConf/work/driver-*/stdout file
        println(s"Root-mean-square error = $rmse")
        userRecs.sort("User_int").show()
        movieRecs.sort("Book_int").show()

        //output is userID, ([Book_int, review/score prediction])
        userRecs.rdd.map(_.toString()).saveAsTextFile(args(1))
        movieRecs.rdd.map(_.toString()).saveAsTextFile(args(2))

    }
}