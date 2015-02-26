package cs6240.proj;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HCount {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = Utilities.getConfiguration(Constants.LOCAL);
		HTable seen = new HTable(conf, Constants.SEEN_TABLE_NAME);
		Scan scan = new Scan();
		ResultScanner rs = seen.getScanner(scan);
		int count = 0;
		for (Result r : rs) {
			count ++;
		}
		
		System.out.println("seen: " + count);
		seen.close();
	}
}
