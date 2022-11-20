import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class RecommenderMapReduce extends Configured implements Tool{

	public static class InputMapper extends Mapper<Object, BytesWritable, Text, Text> {
        private Text value ;

	    public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {

			Text bookTitle, userID;

			String rawText = new String(bWriteable.getBytes());
			BufferedReader bufReader = new BufferedReader(new StringReader(rawText));

			String line=null;
			while((line=bufReader.readLine()) != null ) {

			  Review review = new Review(line);

			  if(!review.getPositiveReview()){
				continue;
			  }

			  bookTitle = new Text("A" + review.getBookTitle());
			  userID = new Text(review.getUserID());

			  context.write(bookTitle, userID); 
			}
	   }
	}
	
    public static class Job3Mapper extends Mapper<Object, BytesWritable, Text, Text> {
        private Text value;

	    public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {
            
	    	String rawText = new String(bWriteable.getBytes());
			BufferedReader bufReader = new BufferedReader(new StringReader(rawText));
			
			String line=null;
			while((line=bufReader.readLine()) != null ) {
        
				String[] curLine = line.split("\\s+");
				
				Text bookTitle = new Text(curLine[0]);
				String recommendation1 = curLine[1];
				String recommendation2 = curLine[2];
				String recommendation3 = curLine[3];
				
				Text recomendations = new Text("B" + recommendation1 + " " + recommendation2 + " " + recommendation3);
				
				context.write(bookTitle, recomendations);
			}
	    }
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
	
        private ArrayList<Text> input = new ArrayList<Text>();
        private ArrayList<Text> job3 = new ArrayList<Text>();
        private Text IDKey = new Text();
        private static final Text EMPTY_TEXT = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
            input.clear();
            job3.clear();
            IDKey.clear();
            
            IDKey = key;
            for (Text text : values) {
                if (text.charAt(0) == 'A') {
                    input.add(new Text(text.toString().substring(1)));
                } 
                else if (text.charAt(0) == 'B') {
                    job3.add(new Text(text.toString().substring(1)));
                }
            }
            executeJoinLogic(context);
		}
		
		private void executeJoinLogic(Context context) throws IOException, InterruptedException {
			
		
            if (!input.isEmpty() && !job3.isEmpty()) {
                for (Text UserID : input) {//go through users who positively reviewed this book(key)
                    for (Text Recommendations : job3) {//Should just be 1 set of recomendations
				
						context.write(UserID, Recommendations);
					
					} 
                }
            }
		}
	}

	public static int runJob(Configuration conf, String inputDirArticles, String inputDirPartA, String outputDir) throws Exception {
		
		Job job = Job.getInstance(conf, "ngram");

		job.setJarByClass(RecommenderMapReduce.class);

		job.setReducerClass(RecommenderMapReduce.IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		MultipleInputs.addInputPath(job, new Path(inputDirArticles), WholeFileInputFormat.class, InputMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputDirPartA), WholeFileInputFormat.class, Job3Mapper.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new RecommenderMapReduce(), args);
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
