package cs6240.proj;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceInit
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
		<ComparableSet, NullWritable, Text, Text> {
		public void reduce(ComparableSet set, Iterable<NullWritable> nulls, 
				Context context
				) throws IOException, InterruptedException {
			// Eliminate duplicates
			ArrayList<ComparableSet> sets = new ArrayList<ComparableSet>();
			for (NullWritable n : nulls) {
				sets.add(new ComparableSet(set));
			}
			ComparableSet friend = Utilities.removeDups(sets);
			if (friend != null) {
				context.write(new Text(String.valueOf(friend.id1)), new Text(String.valueOf(friend.id2)));
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
		Job job = new Job(conf, "InitPhase1");
		job.setJarByClass(ReduceInit.class);
		job.setMapperClass(PopulateMapper.class);
		job.setReducerClass(PopulateReducer.class);
		job.setPartitionerClass(EdgesPopulatePartitioner.class);
		job.setGroupingComparatorClass(EdgesPopulateGroupingComparator.class);
		job.setSortComparatorClass(EdgesPopulateKeyComparator.class);
		job.setMapOutputKeyClass(ComparableSet.class);
		job.setMapOutputValueClass(NullWritable.class);
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