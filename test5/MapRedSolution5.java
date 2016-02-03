package solutions.assignment5;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
 https://cornercases.wordpress.com/2011/08/18/hadoop-object-reuse-pitfall-all-my-reducer-values-are-the-same/
 */
public class MapRedSolution5 /*extends Configured implements Tool*/
{
	
	
	
	public static class Debugger{
		public static boolean debug = false;
		private Debugger(){}
		public static void println(String msg){
			if(debug) System.out.println(msg);
		}
	}
	
	
	public static abstract class JoiningMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

	    private int keyIndex;
	    private Integer returnIndex;
	    private Splitter splitter;
	    private TaggedKey taggedKey = new TaggedKey();
	    private int joinOrder;
	    private Text data = new Text();
	    //private boolean eraseNotKeys;
	    
	    
	    //public JoiningMapper(int step) { this.step= step; }
	    public JoiningMapper() {
	    	//this.eraseNotKeys = eraseNotKeys;
	    }
	    
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	
	    	//FileSplit fileSplit = (FileSplit)context.getInputSplit();
	    	//String id = context.getConfiguration().get(fileSplit.getPath().getName());
	    	//joiner = Joiner.on(separator);
	    	
	    	keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex"+joinOrder));
	    	returnIndex = Integer.parseInt(context.getConfiguration().get("returnIndex"+joinOrder));
	    	String separator = context.getConfiguration().get("separator");
	        
	        splitter = Splitter.on(separator).trimResults();
	        
	        
	    }

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	//String joinKey = values.remove(keyIndex);
	        //String valuesWithOutKey = joiner.join(values);
	    	/*
	        logger = new Jdk14Logger("JoiningReducer");
	        DNSRecordIO newRec = new DNSRecordIO();
	    	newRec.setRawRecord(DNSRecord.createRecord(valuesJoined, logger));
	    	String valuesJoined = joiner.join(values);
	    	*/
	    	
	    	List<String> values = Lists.newArrayList(splitter.split(value.toString()));
	    	
	    	String joinKey = values.get(keyIndex);
	    	
	        taggedKey.set(joinKey, joinOrder);
	        
	        data.set((returnIndex >= 0 ? values.get(returnIndex) : value.toString()));
	        
	        context.write(taggedKey, data);
	        
	        Debugger.println("Mapper "+joinKey+" "+joinOrder+" "+data.toString());
	        
	    }

	}
	
	public static class JoiningMapperFirst extends JoiningMapper {
		public JoiningMapperFirst(){
			super.joinOrder = 1;
		}
	}
	
	/*public static class JoiningMapperFirstEraseNotKeys extends JoiningMapper {
		public JoiningMapperFirstEraseNotKeys(){
			super(true);
			super.joinOrder = 1;
		}
	}*/
	
	public static class JoiningMapperSecond extends JoiningMapper {
		public JoiningMapperSecond(){
			super.joinOrder = 2;
		}
	}
	
	/*public static class JoiningMapperSecondEraseNotKeys extends JoiningMapper {
		public JoiningMapperSecondEraseNotKeys(){
			super(true);
			super.joinOrder = 2;
		}
	}*/
	
	
	
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
	    	//Text[] ite = Iterables.toArray(values, Text.class);
	    	
	    	builder.setLength(0);
	    	
	    	int length = 0;		
	    	for(Text rec: values){
	    		Debugger.println("--->"+rec.toString());
	    		builder.append(rec.toString());
	    		builder.append(";");
	    		length++;
	    	}
	    	
	    	if(length != joinCardinality) 
	    		return;
	    	
	        builder.setLength(builder.length()-1);
	        joinedText.set(builder.toString());
	        context.write(nullKey, joinedText);
	        
	    	Debugger.println("Reducer "+key.getTag().get()+" | "+joinCardinality+" | "+key.getJoinKey().toString()+" | "+Iterables.size(values)+" | "+joinedText);

	    }
	    
	}
    
    public static void main(String[] args) throws Exception
    {
    	//args = new String[]{"res/sampleData/DNS/cnameRRs.dns", "Solution5"};
    	
    	
		
        
    	
        
        
    	
    	
    	JobConf conf1 = new JobConf();
    	JobConf conf2 = new JobConf();
    	JobConf conf3 = new JobConf();
    	
    	String[] otherArgs = null;
    	{
	    	otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
	    	otherArgs = new GenericOptionsParser(conf2, args).getRemainingArgs();
	    	otherArgs = new GenericOptionsParser(conf3, args).getRemainingArgs();
	    	
	    	if (otherArgs.length != 2)
	        {
	            System.err.println("Usage: MapRedSolution5 <in> <out>");
	            System.exit(2);
	        }
    	}
    	
    	final String ALL_DATA = "-1";
    	
    	Path input = new Path(otherArgs[0]);
    	Path output = new Path(otherArgs[1]);
    	
    	MapRedFileUtils.deleteDir(otherArgs[1]);
    	
    	
    	
    	
    	
    	
    	
    	
    	
        conf1.set("keyIndex1", "5");
        conf1.set("returnIndex1", "1");
        conf1.set("keyIndex2", "1");
        //conf1.set("returnIndex2", "5");
        conf1.set("returnIndex2", "1");
        conf1.set("separator", ";");
        conf1.set("join.cardinality", "2");
        
        Job job1 = Job.getInstance(conf1, "MapRed Solution #5.1");
        
        
        MultipleInputs.addInputPath(job1, input, TextInputFormat.class, JoiningMapperFirst.class);
        
        String newLnkName = input.getParent()+"/"+"tmp";
        Path secondary = new Path(newLnkName);
        
        java.nio.file.Path f1 = FileSystems.getDefault().getPath(input.getParent().toUri().getPath(), input.getName());
        java.nio.file.Path f2 = FileSystems.getDefault().getPath(secondary.getParent().toUri().getPath(), secondary.getName());
        Files.deleteIfExists(f2);
        Files.createSymbolicLink(f2, f1);
        
        MultipleInputs.addInputPath(job1, secondary, TextInputFormat.class, JoiningMapperSecond.class);
        
        FileOutputFormat.setOutputPath(job1, new Path(output.toUri().getPath()+"/part1"));
        
        job1.setReducerClass(JoiningReducer.class);
        job1.setPartitionerClass(TaggedJoiningPartitioner.class);
        job1.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        
        job1.setOutputKeyClass(TaggedKey.class);
        job1.setOutputValueClass(Text.class);
        
        ControlledJob ctrlJob1 = new ControlledJob(conf1);
        
        
        
        
        
        
        
        
        
        
        /*
        conf2.set("keyIndex1", "16");
        conf2.set("keyIndex2", "1");
        */
        conf2.set("keyIndex1", "1");
        conf2.set("returnIndex1", ALL_DATA);
        conf2.set("keyIndex2", "1");
        //conf2.set("returnIndex2", "5");
        conf2.set("returnIndex2", "5");
        conf2.set("separator", ";");
        //conf2.set("join.cardinality", "3");
        conf2.set("join.cardinality", "2");
        
        Job job2 = Job.getInstance(conf2, "MapRed Solution #5.2");
        
        //secondary = new Path(output.toUri().getPath()+"/part1/part-r-00000");
        secondary = new Path(output.toUri().getPath()+"/part1");
        MultipleInputs.addInputPath(job2, secondary, TextInputFormat.class, JoiningMapperFirst.class);
        
        MultipleInputs.addInputPath(job2, input, TextInputFormat.class, JoiningMapperSecond.class);
        
        FileOutputFormat.setOutputPath(job2, new Path(output.toUri().getPath()+"/part2"));
        
        job2.setReducerClass(JoiningReducer.class);
        job2.setPartitionerClass(TaggedJoiningPartitioner.class);
        job2.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        
        job2.setOutputKeyClass(TaggedKey.class);
        job2.setOutputValueClass(Text.class);
        
        ControlledJob ctrlJob2 = new ControlledJob(conf2);
        
        
        
        
        
        
        
        
        
        /*
        conf3.set("keyIndex1", "27");
        conf3.set("keyIndex2", "1");
        */
        conf3.set("keyIndex1", "2");
        conf3.set("returnIndex1", ALL_DATA);
        conf3.set("keyIndex2", "1");
        //conf3.set("returnIndex2", "1");
        conf3.set("returnIndex2", "5");
        conf3.set("separator", ";");
        //conf3.set("join.cardinality", "4");
        conf3.set("join.cardinality", "2");
        
        Job job3 = Job.getInstance(conf3, "MapRed Solution #5.3");
        
        //secondary = new Path(output.toUri().getPath()+"/part2/part-r-00000");
        secondary = new Path(output.toUri().getPath()+"/part2");
        MultipleInputs.addInputPath(job3, secondary, TextInputFormat.class, JoiningMapperFirst.class);
        
        MultipleInputs.addInputPath(job3, input, TextInputFormat.class, JoiningMapperSecond.class);
        
        FileOutputFormat.setOutputPath(job3, new Path(output.toUri().getPath()+"/part3"));
        
        job3.setReducerClass(JoiningReducer.class);
        job3.setPartitionerClass(TaggedJoiningPartitioner.class);
        job3.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        
        job3.setOutputKeyClass(TaggedKey.class);
        job3.setOutputValueClass(Text.class);
        
        ControlledJob ctrlJob3 = new ControlledJob(conf3);
        
        
        
        
        
        
       
        ctrlJob1.setJob(job1);
        ctrlJob2.setJob(job2);
        ctrlJob3.setJob(job3);
        JobControl jbcntrl = new JobControl("jbcntrl");
        jbcntrl.addJob(ctrlJob1);
        jbcntrl.addJob(ctrlJob2);
        jbcntrl.addJob(ctrlJob3);
        ctrlJob2.addDependingJob(ctrlJob1);
        ctrlJob3.addDependingJob(ctrlJob2);
        jbcntrl.run();
        
        
        
        
        
        
        
        
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
        
        /*ControlledJob ctrlJob1 = new ControlledJob();
        ControlledJob ctrlJob2 = new ControlledJob();
        ControlledJob ctrlJob3 = new ControlledJob();
        
        JobControl jbcntrl=new JobControl("jbcntrl");
        jbcntrl.addJob(ctrlJob1);
        jbcntrl.addJob(ctrlJob2);
        jbcntrl.addJob(ctrlJob3);
        ctrlJob2.addDependingJob(ctrlJob1);
        ctrlJob3.addDependingJob(ctrlJob2);
        jbcntrl.run();
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
        
    }

	/*@Override
	public int run(String[] args) throws Exception {
		Debugger.println("hey");
		return 0;
	}*/
    
}
