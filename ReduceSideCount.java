package cs6240.proj;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideCount {
	public static String sourceUser = "11929";
	
	public static class CountMapper 
    extends Mapper<Object, Text, Text, NullWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			String[] sp = value.toString().split("	");
			if (sp.length == 2) {
				if (sp[0].equals(sourceUser)) {
					context.write(new Text(sp[1]), NullWritable.get());
				}
				else if (sp[1].equals(sourceUser)) {
					context.write(new Text(sp[0]), NullWritable.get());
				}
			}
		}
	}
	
	public static class CountReducer 
	  extends Reducer<Text,NullWritable,Text,NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> nulls, 
	              Context context
	              ) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	      System.err.println("Usage: ReduceSideCount <in> <out> <source>");
	      System.exit(2);
	    }
	    sourceUser = otherArgs[2];
	    Job job = new Job(conf, "ReduceSideCount");
	    job.setJarByClass(ReduceSideCount.class);
	    job.setMapperClass(CountMapper.class);
	    job.setReducerClass(CountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
