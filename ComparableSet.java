package cs6240.proj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ComparableSet implements WritableComparable<ComparableSet>{
	public int id1;
	public int id2;
	
	public ComparableSet() {
		this.id1 = 0;
		this.id2 = 0;
	}
	
	public ComparableSet(int id1, int id2) {
		this.id1 = id1;
		this.id2 = id2;
	}
	
	public ComparableSet(ComparableSet other) {
		this.id1 = other.id1;
		this.id2 = other.id2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id1 = in.readInt();
		this.id2 = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id1);
		out.writeInt(this.id2);
	}

	@Override
	public int compareTo(ComparableSet other) {
		int id1_delta = this.id1 - other.id1;
		int id2_delta = this.id2 - other.id2;
		int result = 0;
		if (id1_delta != 0) {
			result = id1_delta;
		}
		else if (id2_delta != 0) {
			result = id2_delta;
		}
		return result;
	}
	
	/*
	public int groupCompareTo(ComparableSet other) {
		int thisHash = this.hashCode();
		int otherHash = other.hashCode();
		if (thisHash != otherHash) {
			return thisHash - otherHash;
		}
		else if (this.id1 != other.id2){
			return 0;
		}
		else {
			// duplicate
			return -1;
		}
	}
	*/
	
	public boolean isMirror(ComparableSet other) {
		if ((this.id1 == other.id2) && (this.id2 == other.id1)) {
			return true;
		}
		return false;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ComparableSet) {
			ComparableSet other = (ComparableSet) obj;
			return (this.compareTo(other) == 0?true:false);
		}
		else {
			return false;
		}	
	}
	
	@Override
	public int hashCode() {
		int large = this.id1 > this.id2?id1:id2;
		int small = this.id1 > this.id2?id2:id1;
		String identity = small + "," + large;
		return identity.hashCode();
	}
	
	@Override
	public String toString() {
		return this.id1 + "," + this.id2;
	}
}
