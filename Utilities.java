package cs6240.proj;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Utilities {
	
	public static Configuration getConfiguration(boolean local) throws MasterNotRunningException, ZooKeeperConnectionException {
		if (local) {
			return getLocalConfiguration();
		}
		else {
			return getEMRConfiguration();
		}
	}
	
	public static Configuration getEMRConfiguration() throws MasterNotRunningException, ZooKeeperConnectionException {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("/home/hadoop/hbase/conf/hbase-site.xml");
		HBaseAdmin.checkHBaseAvailable(conf);
		return conf;
	}
	
	public static Configuration getLocalConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", Constants.HBASE_IP);
		conf.set("hbase.master", Constants.HBASE_IP + ":" + Constants.HBASE_PORT);
		return conf;
	}
	
	/*
	 * If mirrored set is found, return set.
	 */
	public static ComparableSet removeDups(List<ComparableSet> sets) {
		if (sets.size() == 0) {
			return null;
		}
		ComparableSet set = sets.get(0);
		for (ComparableSet s : sets) {
			if (s.isMirror(set)) {
				return set;
			}
		}
		return null;
	}
	
	public static byte[] buildRowKey(int id1, int id2) {
		return Bytes.toBytes(fillRowKey(Integer.toString(id1)) + 
							"_" + 
							fillRowKey(Integer.toString(id2)));
	}
	
	/*
	 * Fill row keys with zeroes.
	 */
	public static String fillRowKey(String rowKey) {
		int zeros = Constants.ROWKEY_LENGTH - rowKey.length();
		for (int i=0; i<zeros; i++) {
			rowKey = "0" + rowKey;
		}
		return rowKey;
	}
	
	public static void createTable(
			String tableName, 
			String[] columns, 
			byte[] startKey, 
			byte[] endKey, 
			int split, 
			boolean local) throws IOException {
		Configuration conf = null;
		if (local) {
			conf = getLocalConfiguration();
		}
		else {
			conf = getEMRConfiguration();
		}
		HBaseAdmin admin = new HBaseAdmin(conf);
		// Create new table
		if (admin.tableExists(tableName)) {
			System.out.println("Table\"" + tableName + "\" exists! Delete old table");
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String column : columns) {
			desc.addFamily(new HColumnDescriptor(column));
		}
		admin.createTable(desc, startKey, endKey, split);
		System.out.println("Table \"" + tableName + "\" created!");
		admin.close();
	}
	
	public static void createTableNoSplit(
			String tableName, 
			String[] columns, 
			boolean local) throws IOException {
		Configuration conf = null;
		if (local) {
			conf = getLocalConfiguration();
		}
		else {
			conf = getEMRConfiguration();
		}
		HBaseAdmin admin = new HBaseAdmin(conf);
		// Create new table
		if (admin.tableExists(tableName)) {
			System.out.println("Table\"" + tableName + "\" exists! Delete old table");
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String column : columns) {
			desc.addFamily(new HColumnDescriptor(column));
		}
		admin.createTable(desc);
		System.out.println("Table \"" + tableName + "\" created!");
		admin.close();
	}
	
	public static void addToTable(String tableName, String rowKey, String column, boolean local) throws IOException {
		Configuration conf = null;
		if (local) {
			conf = getLocalConfiguration();
		}
		else {
			conf = getEMRConfiguration();
		}
		HTable table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(column), null, Bytes.toBytes(""));
		table.put(put);
		table.close();
	}
}
