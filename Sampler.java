package cs6240.proj;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sampler {
	public static double percentage = 50.0;
	
	public static class SampleMapper 
    extends Mapper<Object, Text, Text, NullWritable>{
		
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random rand = new Random();
			
			// System.out.println(percentage);
			
			if (rand.nextInt(100) < percentage) {
				context.write(value, NullWritable.get());
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	      System.err.println("Usage: Sampler <in> <out> <percentage>");
	      System.exit(2);
	    }
	    percentage = Double.parseDouble(otherArgs[2]);
	    Job job = new Job(conf, "Sampler");
	    job.setJarByClass(Sampler.class);
	    job.setMapperClass(SampleMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
