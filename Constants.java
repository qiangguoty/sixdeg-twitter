package cs6240.proj;

import org.apache.hadoop.hbase.util.Bytes;

public class Constants {
	public static final int ROWKEY_LENGTH = 8;
	public static final String TABLE_NAME = "friends";
	public static final String SEEN_TABLE_NAME = "seen";
	public static final String INPUT_TABLE_NAME = "input";
	public static final String HBASE_IP = "10.0.0.2";
	public static final String HBASE_PORT = "59058";
	public static final String TEST_INPUT_ID = "00000041";
	public static final boolean LOCAL = false;
	public static final boolean ENABLE_AUTOFLUSH = false;
	public static final String DUMMY = "d";
	public static final byte[] DUMMY_BYTES = Bytes.toBytes("d");
	public static final byte[] NIL_BYTES = Bytes.toBytes("");
}
