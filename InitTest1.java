package cs6240.proj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InitTest1
{
	public static class PopulateMapper 
	extends Mapper<Object, Text, Text, Text>{
		public void map(Object offset, Text value, Context context
				) throws IOException, InterruptedException {
			String [] sp = value.toString().split(",");
			context.write(new Text(sp[0]), new Text(sp[1]));
			context.write(new Text(sp[1]), new Text(sp[0]));
		}
	}


	public static class PopulateReducer extends Reducer
		<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			String k = key.toString();
			HashSet<String> elems = new HashSet<String>();
			List<String> dups = new ArrayList<String>();
			for (Text v : values) {
				String sv = v.toString();
				if (elems.contains(sv)) {
					dups.add(sv);
				}
				else {
					elems.add(sv);
				}
			}
			
			for (String s : dups) {
				context.write(new Text(k), new Text(s));
			}
			
		}
	}
	
	

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();

		// args[0] - input dir
		// args[1] - output dir
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.exit(2);
		}
		
		String inputDir = otherArgs[0];
		String outputDir = otherArgs[1];

		// Hadoop configuration
		Job job = new Job(conf, "InitTest1");
		job.setJarByClass(InitTest1.class);
		job.setMapperClass(PopulateMapper.class);
		job.setReducerClass(PopulateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    
	    boolean result = job.waitForCompletion(true);
	    if (! result) {
	    	System.err.println("---job failed!---");
	    	System.exit(1);
	    }
		System.exit(0);
	}
}