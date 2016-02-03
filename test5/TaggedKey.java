package solutions.assignment5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();

    public TaggedKey() {
		// If we do not provide a constructor java.lang.Exception: java.lang.RuntimeException: java.lang.NoSuchMethodException:
	}
    
    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0){
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
       return compareValue;
    }
    
	@Override
	public void write(DataOutput out) throws IOException {
		joinKey.write(out);
		tag.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		joinKey.readFields(in);
		tag.readFields(in);
	}
	
	public IntWritable getTag() {
		return tag;
	}
	
	public void set(String joinKey, int joinOrder) {
		this.joinKey.set(joinKey);
		this.tag.set(joinOrder);
	}

	public Text getJoinKey() {
		return joinKey;
	}
 }