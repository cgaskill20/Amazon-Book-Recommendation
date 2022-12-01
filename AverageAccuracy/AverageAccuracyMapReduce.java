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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.util.*;
import java.io.BufferedReader;
import java.io.StringReader;

public class AverageAccuracyMapReduce extends Configured implements Tool{

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        double num = Integer.parseInt(value.toString());
		context.write(new IntWritable(1), new DoubleWritable(num));
				}
			}

	   
	

	public static class IntSumReducer extends Reducer<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
				double count = 0;
				double total = 0;
				for(DoubleWritable val : values){
					count++;
					total = total + val.get();
				}
				NullWritable nw = NullWritable.get();
				double avg = total / count;
				context.write(nw, new DoubleWritable(avg));
			}
		}


	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {

		Job job = Job.getInstance(conf, "bigram");

		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(AverageAccuracyMapReduce.class);

		job.setMapperClass(AverageAccuracyMapReduce.TokenizerMapper.class);
		job.setReducerClass(AverageAccuracyMapReduce.IntSumReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new AverageAccuracyMapReduce(), args);
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
