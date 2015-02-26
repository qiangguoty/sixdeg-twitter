package cs6240.proj;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin {
	public static String sourceUser = "11929";
	
	public static class JoinMapper 
    extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			String[] sp = value.toString().split("	");
			if (sp.length == 2) {
				context.write(new Text(sp[0]), new Text(sp[1]));
			}
		}
	}
	
	public static class JoinReducer 
	  extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, 
	              Context context
	              ) throws IOException, InterruptedException {
			String sk = key.toString();
			boolean hasSourceUser = false;
			HashSet<String> collected = new HashSet<String>();
			for (Text v : values) {
				String sv = v.toString();
				if (sv.equals(sourceUser)) {
					hasSourceUser = true;
				}
				if (! collected.contains(sv)) {
					collected.add(sv);
				}
			}
			
			if (hasSourceUser) {
				for (String s : collected) {
					if (! s.equals(sourceUser)) {
						context.write(new Text(sourceUser), new Text(s));
						context.write(new Text(s), new Text(sourceUser));
					}
				}
			}
			for (String s : collected) {
				context.write(new Text(sk), new Text(s));
				context.write(new Text(s), new Text(sk));
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	      System.err.println("Usage: Join <in> <out> <source>");
	      System.exit(2);
	    }
	    sourceUser = otherArgs[2];
	    Job job = new Job(conf, "Join");
	    job.setJarByClass(ReduceSideJoin.class);
	    job.setMapperClass(JoinMapper.class);
	    job.setReducerClass(JoinReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
