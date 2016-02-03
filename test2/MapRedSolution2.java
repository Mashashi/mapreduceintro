package solutions.assignment2;

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

import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;
import solutions.assignment1.ADDR_TYPE;
import examples.MapRedFileUtils;

public class MapRedSolution2
{
    public static class MapRecords extends Mapper<Text, DNSRecordIO, Text, IntWritable>
    {
        private final static IntWritable one;
        private Text word = new Text();

        static{
            one = new IntWritable(1);
        }

        @Override
        protected void map(Text key, DNSRecordIO value, Context context)
            throws IOException, InterruptedException
        {
        	
        	String rdata = value.getRdata().toString();
        	boolean isNetAddr = InetAddresses.isInetAddress(rdata);
        	/*
        	 solutions.AddrType
     			IP=44555
     			URL=120221 
        	 */
        	context.getCounter(isNetAddr?ADDR_TYPE.IP:ADDR_TYPE.URL).increment(1);
            if(isNetAddr){
                word.set(rdata);
                context.write(word, one);
            }
            
        }
    }
    
    /*public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one;
        private Text word = new Text();
        private static Pattern p;

        static{
            p = Pattern.compile(";A;[^;]+;[^;]+;([^;]+);?");
            one = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            Matcher m = p.matcher(value.toString());
            while(m.find()){
                word.set(m.group(1));
                context.write(word, one);
            }
        }
    }*/
    
    public static class ReduceRecords extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
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

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #2");
        
        /* START - your code goes in here*/
        
        job.setInputFormatClass(DNSFileInputFormat.class);
        
        job.setMapperClass(MapRecords.class);
        job.setCombinerClass(ReduceRecords.class);
        job.setReducerClass(ReduceRecords.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        /* END - your code goes in here*/
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
