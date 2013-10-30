package CloudComputing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.Writable;

public class ValueData implements Writable, Comparable<ValueData> {

	private String eventId;
	private String time;
	private String cellId;

	public ValueData(){}
	
    public ValueData(String eventId, String time, String cellId) throws ParseException{
		this.eventId = eventId;
		this.time = time;
		this.cellId = cellId;
	}
	
	public String getEventId(){return eventId;}
	public String getTime(){return time;}
	public String getCellId(){return cellId;}
	
	public int getSeconds() throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		Calendar cal = new GregorianCalendar();
		Date date = sdf.parse(time);
		cal.setTime(date);
		return cal.get(Calendar.SECOND) + cal.get(Calendar.MINUTE)*60 + cal.get(Calendar.HOUR_OF_DAY) * 60*60;
	}
	
	public int getMinutes() throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		Calendar cal = new GregorianCalendar();
		Date date = sdf.parse(time);
		cal.setTime(date);
		return cal.get(Calendar.MINUTE) + cal.get(Calendar.HOUR_OF_DAY) * 60;
	}

	public void readFields(DataInput in) throws IOException {
		eventId = in.readUTF();
		time = in.readUTF();
		cellId = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(eventId);
		out.writeUTF(time);
		out.writeUTF(cellId);
	}
	
	public static ValueData read(DataInput in) throws IOException {
		ValueData vd = new ValueData();
		vd.readFields(in);
		return vd;
	}
	
	public int compareTo(ValueData vd2){
		int vd1Seconds = 0;
		int vd2Seconds = 0;
		try {
			vd1Seconds = this.getSeconds();
			vd2Seconds = vd2.getSeconds();
		} catch(ParseException e){
			System.out.println(e.getMessage());
		}
		
		if(vd1Seconds < vd2Seconds) {
			return -1;
		} else if(vd1Seconds == vd2Seconds) {
			return 0;
		} else {
			return 1;	
		}
		
	}

}

