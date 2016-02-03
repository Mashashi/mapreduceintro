package solutions.assignment5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedJoiningGroupingComparator extends WritableComparator {


    public TaggedJoiningGroupingComparator() {
        super(TaggedKey.class,true);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        TaggedKey taggedKey1 = (TaggedKey)a;
        TaggedKey taggedKey2 = (TaggedKey)b;
        
        int result = -1;
        if(taggedKey1.getTag().get()!=taggedKey2.getTag().get()){
        	result = taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
        }
        return result;
    }
}