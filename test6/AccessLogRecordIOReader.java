package solutions.assignment6;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class AccessLogRecordIOReader extends RecordReader<LongWritable, AccessLogRecordIO>{
	
	private LineRecordReader lineRecordReader;
	private AccessLogRecordIO accessLogRecordIO;
	private static final Log log = LogFactory.getLog(AccessLogRecordIOReader.class);
	
	public AccessLogRecordIOReader() {
		lineRecordReader = new LineRecordReader();
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		lineRecordReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		 boolean retvalue = lineRecordReader.nextKeyValue();

        if (retvalue)
        {
            Text line = lineRecordReader.getCurrentValue();
            accessLogRecordIO = AccessLogRecordIO.createRecord(line.toString(), log);
        }

        return retvalue;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return lineRecordReader.getCurrentKey();
	}

	@Override
	public AccessLogRecordIO getCurrentValue() throws IOException, InterruptedException {
		return accessLogRecordIO;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		lineRecordReader.close();
	}

	
	
}
