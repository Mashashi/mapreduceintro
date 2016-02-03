package solutions.assignment7;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import solutions.assignment6.AccessLogRecordIO;
import solutions.assignment6.AccessLogRecordInputFormat;

public class MapRedSolution7
{
	public static class MapRecords extends Mapper<LongWritable /*InputKey*/, AccessLogRecordIO /*InputValue*/, Text /*ContextKey*/, LongWritable /*ContextValue*/>
    {
		
		private final static LongWritable ONE = new LongWritable(1);
		private final Text EMIT = new Text();
		
		@Override
		protected void map(LongWritable key, AccessLogRecordIO value, Context context) throws IOException, InterruptedException {
			String url = value.g().getReferer();
			if(url!=null){
				EMIT.set(url);
				context.write(EMIT, ONE);
			}
		}
		
    }
	
	public static class ReduceRecords extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		public LongWritable result = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			BigInteger sum = new BigInteger("0");
			for(LongWritable value : values){
				sum = sum.add(value.get() == 1 ? BigInteger.ONE : new BigInteger(value.get()+""));
			}
			result.set(sum.longValue());
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
            System.err.println("Usage: MapRedSolution7 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #7");
        
        /* your code goes in here*/
        
        job.setInputFormatClass(AccessLogRecordInputFormat.class);

        job.setMapperClass(MapRecords.class);
        job.setReducerClass(ReduceRecords.class);

		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
