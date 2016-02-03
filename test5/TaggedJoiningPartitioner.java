package solutions.assignment5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TaggedJoiningPartitioner extends Partitioner<TaggedKey,Text> {
	
	public TaggedJoiningPartitioner() {
		// TODO Auto-generated constructor stub
	}
	
    @Override
    public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
        return taggedKey.getJoinKey().hashCode() % numPartitions;
    }
}