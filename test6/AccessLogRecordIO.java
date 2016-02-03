package solutions.assignment6;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;

import examples.apachelogs.AccessLogRecord;
import lombok.AllArgsConstructor;
import lombok.ToString;



@SuppressWarnings("deprecation")
@AllArgsConstructor
@ToString
public class AccessLogRecordIO extends Record {
	
	private AccessLogRecord accessLogRecord;
	//"%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""
	private static Pattern delimiterPattern = Pattern.compile("^(?<IP>[^ ]*?) (?<REMOTELOGNAME>[^ ]*?) (?<USERID>[^ ]*?) \\[(?<REQUESTRECEIVEDTIME>.*?)\\] \"(?<REQUESTLINE>.*?)\" (?<STATUSCODESERVER>[^ ]*?)( (?<SIZERETURNED>[^ ]*?))?( \"(?<REFERER>.*?)\")?( \"(?<USERAGENT>.*?)\")?\\s*$");
	private static SimpleDateFormat dataFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
	
	public AccessLogRecord g(){
		return accessLogRecord;
	}
	
	public static AccessLogRecordIO createRecord(String line, Log log){
		Matcher m = delimiterPattern.matcher(line);
		AccessLogRecord accessLogRecord = null;
		if(m.find()){
			String[] request = m.group("REQUESTLINE").split(" ");
			
			String ip = m.group("IP");
			String username = m.group("USERID");
			String method = request[0];
			Date date;
			try {
				date = dataFormat.parse(m.group("REQUESTRECEIVEDTIME"));
			} catch (ParseException e) { 
				date = null;
				//throw new InvalidParameterException("Invalid line - Date unparsable");
			}
			String uri = request[1];
			int code;
			try{
				code = Integer.parseInt(m.group("STATUSCODESERVER"));
			}catch(NumberFormatException e){
				code = -1;
			}
			long size;
			try{
				size = Long.parseLong(m.group("SIZERETURNED")); //m.group("SIZERETURNED")!=null?m.group("SIZERETURNED"):"-1"
			}catch(NumberFormatException e){
				size = -1;
			}
			String referer = m.group("REFERER");
			String browser = m.group("USERAGENT");
			accessLogRecord = new AccessLogRecord(ip, username, date, method, uri, code, size, referer, browser);
		}else{
			throw new InvalidParameterException("Invalid line - "+line+" - No record found");
		}
		
		return new AccessLogRecordIO(accessLogRecord);
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public void serialize(RecordOutput rout, String tag) throws IOException {
		rout.startRecord(this, tag);
		rout.writeString(accessLogRecord.getIP(), tag);
		rout.writeString(accessLogRecord.getUsername(), tag);
		rout.writeString(dataFormat.format(accessLogRecord.getDate()), tag);
		rout.writeString(accessLogRecord.getMethod(), tag);
		rout.writeString(accessLogRecord.getUri(), tag);
		rout.writeInt(accessLogRecord.getCode(), tag);
		rout.writeLong(accessLogRecord.getSize(), tag);
		rout.writeString(accessLogRecord.getReferer(), tag);
		rout.writeString(accessLogRecord.getBrowser(), tag);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void deserialize(RecordInput rin, String tag) throws IOException {
		rin.startRecord(tag);
		accessLogRecord.setIP(rin.readString(tag));
		accessLogRecord.setUsername(rin.readString(tag));
		try {
			accessLogRecord.setDate(SimpleDateFormat.getInstance().parse(rin.readString(tag)));
		} catch (ParseException e) {
			throw new IOException("Unparsable date");
		}
		accessLogRecord.setMethod(rin.readString(tag));
		accessLogRecord.setUri(rin.readString(tag));
		accessLogRecord.setCode(rin.readInt(tag));
		accessLogRecord.setSize(rin.readLong(tag));
		accessLogRecord.setReferer(rin.readString(tag));
		accessLogRecord.setBrowser(rin.readString(tag));
	}

	@Override
	public int compareTo(Object peer) throws ClassCastException {
		throw new NotImplementedException();
	}
	
}
