package solutions.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.xbill.DNS.Type;

import examples.MapRedFileUtils;
import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;

public class MapRedSolution3
{

    public static class MapRecords extends Mapper<Text, DNSRecordIO, Text, NullWritable>
    {
        private Text record = new Text();
        //private static Pattern p;

        /*static{
            // ftp.panoroman.com.;CNAME;IN;86400;141.76.123.2
            p = Pattern.compile(";([^;]+);CNAME;[^;]+;[^;]+;([^;]+);");
        }*/
        
        //*.apexqld.info.
        @Override
        protected void map(Text key, DNSRecordIO value, Context context) throws IOException, InterruptedException
        {
            /*
            Matcher m = p.matcher(value.toString());
            while(m.find()){
            	String name = m.group(1);
            	String rdata = m.group(2);
            	
                boolean similar = name.contentEquals(rdata);
                context.getCounter(similar?Similar.YES:Similar.NO).increment(1);
                
                if(similar){
                	
                	//nameEmit.set(name);
	                record.set(value.toString());
	                //context.write(nameEmit, value);
	                context.write(record, null);
                }
                
            }
            */
        	Logger logger = Logger.getLogger(MapRecords.class);
        	
        	int id = value.toString().hashCode();
        	logger.debug("Starting record["+id+"]:"+value.toString());
        	
        	if(value.getType().get() != Type.CNAME){
        		context.getCounter(RECORD_INVALID.NOT_CNAME).increment(1);
        		return;
        	}
        	
        	//logger.debug("Record typed["+id+"]:"+Type.CNAME);
        	
        	Text name = value.getName();
    		Text rname = value.getRdata();
    		
        	{
	    		if((name==null || name.toString() == null) && (rname==null||rname.toString()==null)){
	    			context.getCounter(RECORD_INVALID.MISSING_NAME_AND_RNAME).increment(1);
	    			return;
	    		}
	    		if((name==null || name.toString() == null)){
	    			context.getCounter(RECORD_INVALID.MISSING_NAME).increment(1);
	    			return;
	    		}
	    		if((rname==null||rname.toString()==null)){
	    			context.getCounter(RECORD_INVALID.MISSING_RNAME).increment(1);
	    			return;
	    		}
        	}
        	
        	
        	//logger.debug("Record named["+id+"]:"+name);
        	//logger.debug("Record rnamed["+id+"]:"+rname);
        	
        	boolean similar = name.toString().contentEquals(rname.toString());
        	
            context.getCounter(similar?SIMILAR.YES:SIMILAR.NO).increment(1);
            
            if(similar){
                record.set(value.toString());
                context.write(record, NullWritable.get()); // Se meter no valor null o programa rebenta depois do mapeamento
            }
        	
        	
        }
    }
    
    /*public static class ReduceRecords extends Reducer<Text, Text, Text, Text>
    {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String ipList = "";
            for (Text val : values){
                ipList += val.toString()+";";
            }
            result.set(ipList);
            context.write(key, result);
        }
    }*/
    
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution3 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #3");
        
        /* your code goes in here*/
        
        job.setInputFormatClass(DNSFileInputFormat.class);
        
        job.setMapperClass(MapRecords.class);
        //job.setCombinerClass(ReduceRecords.class);
        //job.setReducerClass(ReduceRecords.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
