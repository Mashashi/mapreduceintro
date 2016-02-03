package solutions.assignment6;

import java.io.IOException;

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

/*
 * 
 * https://hadoopi.wordpress.com/2013/05/31/custom-recordreader-processing-string-pattern-delimited-records/
 * 
 * */
public class MapRedSolution6 {
	
	public static class MapRecords extends Mapper<LongWritable /*InputKey*/, AccessLogRecordIO /*InputValue*/, LongWritable /*ContextKey*/, Text /*ContextValue*/>
    {
		@Override
		protected void map(LongWritable key, AccessLogRecordIO value, Context context) throws IOException, InterruptedException {
			/*
			 	private String IP;
			    private String username;
			    private Date date;
			    private String method;
			    private String uri;
			    private int code;
			    private long size;
			    private String referer;
			    private String browser;
			 
			 */
			
			//System.out.println("-->"+value.toString());
			
		}
    }
	
	public static class ReduceRecords extends Reducer<LongWritable, Text, LongWritable, Text>{}
	
	public static void main(String[] args) throws Exception
    {
		Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution6 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #6");
        
        /* START - your code goes in here*/
        
        job.setInputFormatClass(AccessLogRecordInputFormat.class);

        job.setMapperClass(MapRecords.class);
        job.setReducerClass(ReduceRecords.class);

		job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        /* END - your code goes in here*/

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	        
    }
}
