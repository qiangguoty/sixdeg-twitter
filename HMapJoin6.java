

package cs6240.proj;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HMapJoin6 {
	/*
	 * Find friends for each input user id
	 */
	public static class JoinMapper extends Mapper<Object, Text, Text, NullWritable>{
		private static Configuration mapperConf = null;
		private static HTable friends = null;
		private static HTable seen = null;
		private static Get get = null;
		private static Put put = null;
		private static Result r = null;

		public boolean inSeen(String key) throws IOException {
			if (mapperConf != null && seen != null) {
				get = new Get(key.getBytes());
				r = seen.get(get);
				if (r.getRow() == null) {
					return false;
				}
				return true;
			}
			System.err.println("HBase connection fail");
			return false;
		}
		
		public void addToSeen(String key) throws IOException {
			if (mapperConf != null && seen != null){
				put = new Put(key.getBytes());
				put.add(Constants.DUMMY_BYTES, null, Constants.NIL_BYTES);
				seen.put(put);
			}
			else {
				System.err.println("HBase connection fail");
			}
		}
		
		public void setup(Context context) throws IOException {
			mapperConf = Utilities.getConfiguration(Constants.LOCAL);
			friends = new HTable(mapperConf, Constants.TABLE_NAME);
			seen = new HTable(mapperConf, Constants.SEEN_TABLE_NAME);
			friends.setAutoFlush(Constants.ENABLE_AUTOFLUSH);
			seen.setAutoFlush(Constants.ENABLE_AUTOFLUSH);
		}
		
		public String[] getFriends(String key) throws IOException {
			String[] result = null;
			if (mapperConf != null && friends != null) {
				get = new Get(key.getBytes());
				r = friends.get(get);
				for (KeyValue kv : r.raw()) {
					if (new String(kv.getFamily()).equals("friends")) {
						 result = new String(kv.getValue()).split(",");
					}
				}
			}
			else {
				System.err.println("HBase connection fail");
			}
			return result;
		}
		
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fs = getFriends(value.toString());
			if (fs != null) {
				for (String f : fs) {
					addToSeen(f);
					context.write(new Text(f), NullWritable.get());
				}
			}
		}
		
		public void cleanup(Context context) throws IOException {
			friends.close();
			seen.close();
		}
		
	}

	public static class JoinReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = Utilities.getConfiguration(Constants.LOCAL);
		conf.set("mapred.min.split.size", "50000");			// 50KB min chunk size
		conf.set("mapred.max.split.size", "250000");		// 250KB max chunk size
		//conf.setBoolean("mapreduce.map.speculative", false);
		//conf.setBoolean("mapreduce.reduce.speculative", false);
		//conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		//conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

		// args[0] - input dir
		// args[1] - output dir
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Not enough arguments");
			System.exit(2);
		}

		String inputDir = otherArgs[0];
		String outputDir = otherArgs[1];
		
		// Hadoop configuration
		Job job = new Job(conf, "HMapJoin");
		job.setJarByClass(HMapJoin6.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
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
