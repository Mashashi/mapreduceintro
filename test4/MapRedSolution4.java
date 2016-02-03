package solutions.assignment4;

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
import org.xbill.DNS.Type;

import com.google.common.net.InetAddresses;

import examples.MapRedFileUtils;
import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;
import solutions.assignment3.RECORD_INVALID;

public class MapRedSolution4
{

    public static class MapRecords extends Mapper<Text, DNSRecordIO, Text, NullWritable>
    {
        private Text record = new Text();
        
        @Override
        protected void map(Text key, DNSRecordIO value, Context context) throws IOException, InterruptedException
        {
            
        	
        	if(value.getType().get() != Type.CNAME){
        		context.getCounter(RECORD_INVALID.NOT_CNAME).increment(1);
        		return;
        	}
        	
        	
        	//Text name = value.getName();
    		Text rname = value.getRdata();
    		
        	{
	    		/*if((name==null || name.toString() == null) && (rname==null||rname.toString()==null)){
	    			context.getCounter(RECORD_INVALID.MISSING_NAME_AND_RNAME).increment(1);
	    			return;
	    		}
	    		if((name==null || name.toString() == null)){
	    			context.getCounter(RECORD_INVALID.MISSING_NAME).increment(1);
	    			return;
	    		}*/
	    		if((rname==null||rname.toString()==null)){
	    			context.getCounter(RECORD_INVALID.MISSING_RNAME).increment(1);
	    			return;
	    		}
        	}
        	
        	boolean rdataHasIpAddr = rname != null && rname.getLength()> 0 && rname.toString().charAt(rname.toString().length()-1) == '.' &&  InetAddresses.isInetAddress(rname.toString().substring(0, rname.toString().length()-1));
        	
            context.getCounter(rdataHasIpAddr?HAS_IP_ADDR.YES:HAS_IP_ADDR.NO).increment(1);
            
            if(rdataHasIpAddr){
                record.set(value.toString());
                context.write(record, NullWritable.get()); // Se meter no valor null o programa rebenta depois do mapeamento
            }
        	
        	
        }
    }
    
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
