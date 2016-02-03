package solutions.assignment6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;

public class AccessLogRecordInputFormat extends FileInputFormat<LongWritable, AccessLogRecordIO> {

	@Override
	public RecordReader<LongWritable, AccessLogRecordIO> createRecordReader(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
		return new AccessLogRecordIOReader();
	}

}
