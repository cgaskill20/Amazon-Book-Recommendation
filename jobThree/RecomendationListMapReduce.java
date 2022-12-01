import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.*;
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
public class RecomendationListMapReduce extends Configured implements Tool{

	public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, RecomendationWriteable> {

	    public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {

	    	String rawText = new String(bWriteable.getBytes());
			BufferedReader bufReader = new BufferedReader(new StringReader(rawText));

			String line=null;
			while((line=bufReader.readLine()) != null ) {
        
				String[] curLine = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        
				if(curLine.length <= 2){
		  			continue;
        			}
        
				Text firstBook = new Text(curLine[0]);
				Text secondBook = new Text(curLine[1]);
				IntWritable ocurences = new IntWritable(Integer.parseInt(curLine[2].trim()));
				
				RecomendationWriteable recWrite = new RecomendationWriteable(secondBook, ocurences);
				context.write(firstBook, recWrite);
			}
	    }
	}

	public static class IntSumReducer extends Reducer<Text, RecomendationWriteable, Text, Text> {


		public void reduce(Text key, Iterable<RecomendationWriteable> values, Context context) throws IOException, InterruptedException {

			Map<String, Integer> recomendations = new HashMap<String, Integer>();
			String outputValue = ", ";
			
			for (RecomendationWriteable rec : values) {
				recomendations.put(rec.getRecomendationTitle().toString(), rec.getOcurences().get());
			}
			
			for (Entry<String, Integer> entry : entriesSortedByValues(recomendations)) {
				outputValue += entry.getKey() + ", ";
			}
			
			Text value = new Text(outputValue);
			
			context.write(key, value);
		}
		
		static <K,V extends Comparable<? super V>> 
            List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {

			List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());

			Collections.sort(sortedEntries, new Comparator<Entry<K,V>>() {
					@Override
					public int compare(Entry<K,V> e1, Entry<K,V> e2) {
                    return e2.getValue().compareTo(e1.getValue());
					}
				}
			);

			return sortedEntries;
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {

		Job job = Job.getInstance(conf);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setJarByClass(RecomendationListMapReduce.class);

		job.setMapperClass(RecomendationListMapReduce.TokenizerMapper.class);
		//job.setCombinerClass(RecomendationListMapReduce.class);
		job.setReducerClass(RecomendationListMapReduce.IntSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RecomendationWriteable.class);

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
		int res = ToolRunner.run(new Configuration(), new RecomendationListMapReduce(), args);
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
