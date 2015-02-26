package cs6240.proj;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InitTest2
{

	public static class PopulateMapper 
	extends Mapper<Object, Text, Text, Text>{
		/*
		 * Input : u1<tab>u2
		 * Emit: (u1, u2)
		 */
		public void map(Object offset, Text value, Context context
				) throws IOException, InterruptedException {
			String [] splitted = value.toString().split("	");
			context.write(new Text(splitted[0]), new Text(splitted[1]));
			// context.write(new Text(splitted[1]), new Text(splitted[0]));
		}
	}


	public static class PopulateReducer extends Reducer
		<Text, Text, ImmutableBytesWritable, Writable> {
		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			// Rowkey: u1
			String filledRowKeyStr = Utilities.fillRowKey(key.toString());
			byte[] rowKey = Bytes.toBytes(filledRowKeyStr);
			String friends = "";
			for (Text v : values) {
				friends = friends + Utilities.fillRowKey(v.toString()) + ",";
			}
			friends = friends.substring(0, friends.length()-1);
			// System.out.println(friends);
			byte[] rowVal = Bytes.toBytes(friends);
			Put put = new Put(rowKey);
			put.add(Bytes.toBytes("friends"), null, rowVal);
			context.write(new ImmutableBytesWritable(rowKey), put);
		}
	}
	
	

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = Utilities.getConfiguration(Constants.LOCAL);

		// args[0] - input dir
		// args[1] - input user ID
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.exit(2);
		}
		
		String testInputId = otherArgs[1];
		if (testInputId == null || testInputId.length() == 0) {
			testInputId = Constants.TEST_INPUT_ID;
		}
		
		String[] dummy = new String[]{Constants.DUMMY};
		String[] friends = new String[]{"friends"};
		// Create & init tables
		int split = 10;
		// friends:imported data
		byte[] startKey = Bytes.toBytes("01000000");
		byte[] endKey = Bytes.toBytes("10000000");
		Utilities.createTable(Constants.TABLE_NAME, friends, startKey, endKey, split, Constants.LOCAL);
		// seen:discovered connections
		startKey = Bytes.toBytes("01000000");
		endKey = Bytes.toBytes("10000000");
		Utilities.createTable(Constants.SEEN_TABLE_NAME, dummy, startKey, endKey, split, Constants.LOCAL);
		
		// initialize seen table
		Utilities.addToTable(Constants.SEEN_TABLE_NAME, testInputId, Constants.DUMMY, Constants.LOCAL);
		
		// Hadoop configuration
		Job job = new Job(conf, "InitTest2");
		job.setJarByClass(InitTest2.class);
		job.setMapperClass(PopulateMapper.class);
		job.setReducerClass(PopulateReducer.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Constants.TABLE_NAME);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    
	    boolean result = job.waitForCompletion(true);
	    if (! result) {
	    	System.err.println("---job failed!---");
	    	System.exit(1);
	    }
		System.exit(0);
	}
}