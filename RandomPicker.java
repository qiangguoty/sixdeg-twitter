package cs6240.proj;

import java.io.IOException;
import java.util.Random;

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

public class RandomPicker {
	public static int limit = 5;
	
	public static class RandomMapper 
    extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			String val = value.toString().split(",")[0];
			Random rand = new Random();
			String k = String.valueOf(rand.nextInt());
			context.write(new Text(k), new Text(val));
		}
	}
	
	public static class RandomReducer
    extends Reducer<Text, Text, Text, NullWritable>{
		public static int limit = 5;
		public static int count = 0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (count < limit) {
				for (Text v : values) {
					System.out.println(v.toString());
					break;
				}
				count = count + 1;
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: RandomPicker <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "RandomPicker");
	    job.setJarByClass(RandomPicker.class);
	    job.setMapperClass(RandomMapper.class);
	    job.setReducerClass(RandomReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
