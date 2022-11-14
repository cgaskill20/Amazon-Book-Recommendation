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

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.util.*;
import java.io.BufferedReader;
import java.io.StringReader;

public class UserToBooksMapReduce extends Configured implements Tool{

	public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, Text> {

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

          bookTitle = new Text(review.getBookTitle());
          userID = new Text(review.getUserID());

          context.write(userID, bookTitle); 
        }
	   }
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String groupedBooks = "";
			boolean first = true;
			int count = 0;
			
			for (Text book : values) {
				
				count++;
				
				if(first){
					groupedBooks = book.toString();
					first = false;
				}
				else{
					groupedBooks += "," + book.toString();
				}
			}
			
			if(count > 1){ //a user with only 1 review doesnt help us
				Text books = new Text(groupedBooks);
				context.write(key, books);
			}
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {

		Job job = Job.getInstance(conf, "ngram");

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setJarByClass(UserToBooksMapReduce.class);

		job.setMapperClass(UserToBooksMapReduce.TokenizerMapper.class);
		//job.setCombinerClass(UserToBooksMapReduce.class);
		job.setReducerClass(UserToBooksMapReduce.IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new UserToBooksMapReduce(), args);
    System.exit(res); //res will be 0 if all tasks are executed succesfully and 1 otherwise
	}

	@Override
   	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
        if(runJob(conf, args[0], args[1].toString()) != 0) 
				return 1; //error
		return 0;	 //success
   	}
}
