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


/*
 * Find all mutual follow relationships from given data,
 * then store to HBase
 * key:'user_id1'_'user_id2'
 * value:dummy
 */
public class Init
{

	public static class PopulateMapper 
	extends Mapper<Object, Text, ComparableSet, NullWritable>{
		/*
		 * Input : user_id1,user_id2
		 * Emit (Set(user_id1,user_id2), null)
		 */
		public void map(Object offset, Text value, Context context
				) throws IOException, InterruptedException {
			String [] splitted = value.toString().split(",");
			context.write(new ComparableSet(Integer.parseInt(splitted[0]), 
											Integer.parseInt(splitted[1])), 
						  NullWritable.get());
		}
	}
	
	public static class EdgesPopulatePartitioner extends Partitioner<ComparableSet, NullWritable> {

		@Override
		public int getPartition(ComparableSet set, NullWritable n, int numOfReduceTasks) {
			return (Math.abs(set.hashCode() % numOfReduceTasks));
		}
	}

	
	public static class EdgesPopulateGroupingComparator extends WritableComparator {

		protected EdgesPopulateGroupingComparator() {
			super(ComparableSet.class, true);
		}
		
		@Override  
		public int compare(WritableComparable w1, WritableComparable w2) {
			ComparableSet set1 = (ComparableSet) w1;
			ComparableSet set2 = (ComparableSet) w2;
			return set1.hashCode() - set2.hashCode();
		}
	}

	public static class EdgesPopulateKeyComparator extends WritableComparator {

		protected EdgesPopulateKeyComparator() {
			super(ComparableSet.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			ComparableSet set1 = (ComparableSet) w1;
			ComparableSet set2 = (ComparableSet) w2;
			return set1.hashCode() - set2.hashCode();
		}
	}


	public static class PopulateReducer extends Reducer
		<ComparableSet, NullWritable, ImmutableBytesWritable, Writable> {
		public void reduce(ComparableSet set, Iterable<NullWritable> nulls, 
				Context context
				) throws IOException, InterruptedException {
			Put put1 = null;
			Put put2 = null;
			// Eliminate duplicates
			ArrayList<ComparableSet> sets = new ArrayList<ComparableSet>();
			for (NullWritable n : nulls) {
				sets.add(new ComparableSet(set));
			}
			ComparableSet friend = Utilities.removeDups(sets);
			if (friend != null) {
				byte[] rowKey1 = Utilities.buildRowKey(friend.id1, friend.id2);
				byte[] rowKey2 = Utilities.buildRowKey(friend.id2, friend.id1);
				put1 = new Put(rowKey1);
				put2 = new Put(rowKey2);
				put1.add(Bytes.toBytes(Constants.DUMMY), null, Constants.NIL_BYTES);
				put2.add(Bytes.toBytes(Constants.DUMMY), null, Constants.NIL_BYTES);
				context.write(new ImmutableBytesWritable(rowKey1), put1);
				context.write(new ImmutableBytesWritable(rowKey2), put2);
			}
		}
	}
	
	

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = Utilities.getConfiguration(Constants.LOCAL);

		// args[0] - input file directory path
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
		// Create & init tables
		int split = 10;
		// friends:imported data
		byte[] startKey = Bytes.toBytes("01000000_00000000");
		byte[] endKey = Bytes.toBytes("10000000_00000000");
		Utilities.createTable(Constants.TABLE_NAME, dummy, startKey, endKey, split, Constants.LOCAL);
		// seen:discovered connections
		startKey = Bytes.toBytes("01000000");
		endKey = Bytes.toBytes("10000000");
		Utilities.createTable(Constants.SEEN_TABLE_NAME, dummy, startKey, endKey, split, Constants.LOCAL);
		
		// input:seeds for each iterations
		// Utilities.createTable(Constants.INPUT_TABLE_NAME, dummy, Constants.LOCAL);
		
		// initialize seen table
		Utilities.addToTable(Constants.SEEN_TABLE_NAME, testInputId, Constants.DUMMY, Constants.LOCAL);
		// initialize input table
		// Utilities.addToTable(Constants.INPUT_TABLE_NAME, "1_" + testInputId, Constants.DUMMY, Constants.LOCAL);
		
		// Hadoop configuration
		Job job = new Job(conf, "Init");
		job.setJarByClass(Init.class);
		job.setMapperClass(PopulateMapper.class);
		job.setReducerClass(PopulateReducer.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setPartitionerClass(EdgesPopulatePartitioner.class);
		job.setGroupingComparatorClass(EdgesPopulateGroupingComparator.class);
		job.setSortComparatorClass(EdgesPopulateKeyComparator.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Constants.TABLE_NAME);
		job.setMapOutputKeyClass(ComparableSet.class);
		job.setMapOutputValueClass(NullWritable.class);
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