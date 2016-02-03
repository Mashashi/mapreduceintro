package solutions.assignment1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.net.InetAddresses;

import examples.dns.DNSRecordIO;
import examples.dns.DNSFileInputFormat;

import examples.MapRedFileUtils;

public class MapRedSolution1
{
    
	/*
	 * OutputValueClass has to be the same as the input 
	 **/
	public static class MapRecords extends Mapper<Text /*InputKey*/, DNSRecordIO/*InputValue*/, IntWritable /*ContextKey*/, IntWritable /*ContextValue*/>
    {
        private final static IntWritable one;
        private IntWritable word = new IntWritable();

        static{
        	one = new IntWritable(1);
        }

        @Override
        protected void map(Text key, DNSRecordIO value, Context context)
            throws IOException, InterruptedException
        {
        	
        	String addr = value.getName().toString();
        	//Total Records = 53172+111604 = 164776
        	context.getCounter(InetAddresses.isInetAddress(addr)?ADDR_TYPE.IP:ADDR_TYPE.URL).increment(1);
        	
        	int level = addr.split("\\.").length;
        	word.set(level);
        	
    		context.write(word, one);
        }
    }
    /*
    // To use this implementation:
    // 1. uncomment this section 
    // 2. comment the actual mapper section 
    // 3. coomment the line job.setInputFormatClass(DNSFileInputFormat.class);
    public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one;
        private Text word = new Text();
        private static Pattern p;

        static{
        	p = Pattern.compile("^([^;]+);");
        	one = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
    		Matcher m = p.matcher(value.toString());
    		while(m.find()){
    			word.set(String.valueOf(m.group(1).split("\\.").length));
    			context.write(word, one);
    		}
        }
    }
    */

    public static class ReduceRecords extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            	sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution1 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #1");
        
        /* START - your code goes in here*/
        
        job.setInputFormatClass(DNSFileInputFormat.class);

        job.setMapperClass(MapRecords.class);
        job.setCombinerClass(ReduceRecords.class);
        job.setReducerClass(ReduceRecords.class);

		job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        /* END - your code goes in here*/

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
