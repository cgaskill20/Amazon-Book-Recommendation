import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.util.*;
import java.io.BufferedReader;
import java.io.StringReader;

public class AccuracyMapReduce extends Configured implements Tool{

	public static class RecommenderMapper extends Mapper<Object, BytesWritable, Text, Text> {
        private Text value ;

	    public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {

			Text userID, recommendedTitles;

			String rawText = new String(bWriteable.getBytes());
			BufferedReader bufReader = new BufferedReader(new StringReader(rawText));

			String line=null;
			while((line=bufReader.readLine()) != null ) {

				String arr[] = line.split(" ", 2);

				userID = new Text("A" +arr[0]);
				recommendedTitles = new Text(arr[1]);

				context.write(userID, recommendedTitles); 
			}
	   }
	}
	
    public static class Job1Mapper extends Mapper<Object, BytesWritable, Text, Text> {
        private Text value;

	    public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {
            
			Text userID, positivelyReviewedTitles;

			String rawText = new String(bWriteable.getBytes());
			BufferedReader bufReader = new BufferedReader(new StringReader(rawText));

			String line=null;
			while((line=bufReader.readLine()) != null ) {

				String arr[] = line.split(" ", 2);

				userID = new Text("B" +arr[0]);
				positivelyReviewedTitles = new Text(arr[1]);

				context.write(userID, positivelyReviewedTitles); 
			}
	    }
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, NullWritable> {
	
        private ArrayList<Text> recommender = new ArrayList<Text>();
        private ArrayList<Text> job1 = new ArrayList<Text>();
        private Text IDKey = new Text();
        private static final Text EMPTY_TEXT = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
            recommender.clear();
            job1.clear();
            IDKey.clear();
            
            IDKey = key;
            for (Text text : values) {
                if (text.charAt(0) == 'A') {
                    recommender.add(new Text(text.toString().substring(1)));
                } 
                else if (text.charAt(0) == 'B') {
                    job1.add(new Text(text.toString().substring(1)));
                }
            }
            executeJoinLogic(context);
		}
		
		private void executeJoinLogic(Context context) throws IOException, InterruptedException {
			
		
            if (!recommender.isEmpty() && !job1.isEmpty()) {
                for (Text recs : recommender) {//go through each recommendation set for this user
					
					String recsString = recs.toString();
					List<String> recommendedTitles = Arrays.asList(recsString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1));
				
                    for (Text positiveReviews : job1) {//Should just be 1 set of positively reviewd books from job 1
						
						String positiveReviewsString = positiveReviews.toString();
						List<String> positivelyReviewedTitlesTitles = Arrays.asList(positiveReviewsString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1));
						
						
						positivelyReviewedTitlesTitles.retainAll(recommendedTitles);
						Text numMatchingRecommendations = new Text(String.valueOf(positivelyReviewedTitlesTitles.size()));
						NullWritable nw = NullWritable.get();
						
						context.write(numMatchingRecommendations, nw);//write once per user
						
						
					
					} 
                }
            }
		}
	}

	public static int runJob(Configuration conf, String recommender, String job1, String outputDir) throws Exception {
		
		Job job = Job.getInstance(conf, "ngram");

		job.setJarByClass(AccuracyMapReduce.class);

		job.setReducerClass(AccuracyMapReduce.IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		MultipleInputs.addInputPath(job, new Path(recommender), WholeFileInputFormat.class, RecommenderMapper.class);
        MultipleInputs.addInputPath(job, new Path(job1), WholeFileInputFormat.class, Job1Mapper.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new AccuracyMapReduce(), args);
        System.exit(res); //res will be 0 if all tasks are executed succesfully and 1 otherwise
	}

	@Override
   	public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        if(runJob(conf, args[0], args[1], args[2].toString()) != 0) //#TODO#: update this
				return 1; //error

		return 0;	 //success
   	}
}
