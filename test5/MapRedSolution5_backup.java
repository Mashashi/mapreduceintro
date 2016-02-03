package solutions.assignment5;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.archive.util.FileUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import examples.MapRedFileUtils;

/*
 http://codingjunkie.net/mapreduce-reduce-joins/ 
 http://stackoverflow.com/questions/2499585/chaining-multiple-mapreduce-jobs-in-hadoop
 http://stackoverflow.com/questions/3207238/where-does-hadoop-mapreduce-framework-send-my-system-out-print-statements-s/5785472#5785472
 http://pt.slideshare.net/shalishvj/map-reduce-joins-31519757
 http://grepcode.com/file/repo1.maven.org/maven2/com.ning/metrics.serialization-all/2.0.0-pre7/org/apache/hadoop/io/Writable.java
 */
public class MapRedSolution5_backup
{
	
	
	
	
	
	public static class JoiningMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

	    private int keyIndex;
	    private Splitter splitter;
	    private Joiner joiner;
	    private TaggedKey taggedKey = new TaggedKey();
	    private int joinOrder;
	    private Text data = new Text();
	    
	    public JoiningMapper() {}
	    
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        
	    	FileSplit fileSplit = (FileSplit)context.getInputSplit();
	    	String id = context.getConfiguration().get(fileSplit.getPath().getName());
	        joinOrder = Integer.parseInt(id);
	    	
	    	keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex"+id));
	        String separator = context.getConfiguration().get("separator");
	        
	        splitter = Splitter.on(separator).trimResults();
	        joiner = Joiner.on(separator);
	        
	    }

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	//String joinKey = values.remove(keyIndex);
	        //String valuesWithOutKey = joiner.join(values);
	    	/*
	        logger = new Jdk14Logger("JoiningReducer");
	        DNSRecordIO newRec = new DNSRecordIO();
	    	newRec.setRawRecord(DNSRecord.createRecord(valuesJoined, logger));
	    	*/
	    	
	    	List<String> values = Lists.newArrayList(splitter.split(value.toString()));
	    	
	    	String joinKey = values.get(keyIndex);
	    	String valuesJoined = joiner.join(values);
	    	
	        taggedKey.set(joinKey, joinOrder);
	        data.set(valuesJoined);
	        
	        context.write(taggedKey, data);
	        
	        System.out.println("Mapper "+joinKey+" "+joinOrder+" "+valuesJoined);
	        
	    }

	}
	
	
	
	
	
	
	
	public static class JoiningReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {

	    private Text joinedText = new Text();
	    private StringBuilder builder = new StringBuilder();
	    private NullWritable nullKey = NullWritable.get();
	    private int joinCardinality;
	    
	    public JoiningReducer() {}
	    
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	joinCardinality = Integer.parseInt(context.getConfiguration().get("join.cardinality"));
	    }
	    
	    /*
	     This is implemented as a inner join
	     */
	    @Override
	    protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	
	    	//Iterables.size(values)
	    	//builder.append(key.getJoinKey()).append(",");
	    	//builder.append(value.toString()).append(",");
	    	//org.apache.commons.logging.Log
	    	
	    	Text[] ite = Iterables.toArray(values, Text.class);
	    	
	    	if(ite.length == joinCardinality) 
	    		return;
	    			
	    	for(Text rec: ite){
	    		builder.append(rec.toString());
	    		builder.append(",");
	    	}
	        builder.setLength(builder.length()-1);
	        joinedText.set(builder.toString());
	        context.write(nullKey, joinedText);
	        builder.setLength(0);
	    	
	        
	        
	        
	    	System.out.println("Reducer "+key.getTag().get()+" | "+joinCardinality+" | "+key.getJoinKey().toString()+" | "+Iterables.size(values)+" | "+joinedText);

	    }
	    
	}
    
    public static void main(String[] args) throws Exception
    {
    	//args = new String[]{"res/sampleData/DNS/cnameRRs.dns", "Solution5"};
    	
    	Configuration conf = new Configuration();
        
        conf.set("keyIndex1", "1");
        conf.set("keyIndex2", "5");
        
        conf.set("separator", ";");
        conf.set("join.cardinality", "2");
        
        
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        		
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution5 <in> <out>");
            System.exit(2);
        }
        
        
        
        File selectedFile = new File(otherArgs[0]);
        
        String newFile = selectedFile.getParent()+"/"+selectedFile.getName()+"2";
        FileUtils.copyFile(new File(otherArgs[0]), new File(newFile));
        
        Splitter splitter = Splitter.on('/');
        String[] fileArgs = {otherArgs[0], newFile};
        StringBuilder filePaths = new StringBuilder();
        
        {
	        for(int i = 0; i< fileArgs.length; i++) {
	            String fileName = Iterables.getLast(splitter.split(fileArgs[i]));
	            conf.set(fileName, Integer.toString(i+1));
	            filePaths.append(fileArgs[i]).append(",");
	        }
	        filePaths.setLength(filePaths.length() - 1);
        }
        
        
        
        Job job = Job.getInstance(conf, "MapRed Solution #5");
        
        System.out.println("Processing "+filePaths.toString());
        FileInputFormat.addInputPaths(job, filePaths.toString());
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        job.setMapperClass(JoiningMapper.class);
        job.setReducerClass(JoiningReducer.class);
        job.setPartitionerClass(TaggedJoiningPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        
        job.setOutputKeyClass(TaggedKey.class);
        job.setOutputValueClass(Text.class);
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

	/*@Override
	public int run(String[] args) throws Exception {
		System.out.println("hey");
		return 0;
	}*/
    
}
