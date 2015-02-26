package cs6240.proj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyPair implements WritableComparable<KeyPair>{
	public int id1;
	public int id2;
	public boolean first;
	
	public KeyPair() {
		this.id1 = 0;
		this.id2 = 0;
		this.first = false;
	}
	
	public KeyPair(int id1, int id2, boolean first) {
		this.id1 = id1;
		this.id2 = id2;
		this.first = first;
	}
	
	public KeyPair(KeyPair other) {
		this.id1 = other.id1;
		this.id2 = other.id2;
		this.first = other.first;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id1 = in.readInt();
		this.id2 = in.readInt();
		this.first = in.readBoolean();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id1);
		out.writeInt(this.id2);
		out.writeBoolean(this.first);
	}

	@Override
	public int compareTo(KeyPair other) {
		int result = this.id1 < other.id1?-1:1;
		if ((result == 0) && (this.id2 != other.id2)) {
			result = this.id2 < other.id2?-1:1;
		}
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof KeyPair) {
			KeyPair other = (KeyPair) obj;
			return (this.id1 == other.id1 && this.id2 == other.id2);
		}
		else {
			return false;
		}	
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	@Override
	public String toString() {
		return this.id1 + "," + this.id2 + "," + this.first;
	}
}
