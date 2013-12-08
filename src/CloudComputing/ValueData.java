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
	
    public ValueData(String eventId, String time, String cellId){
		this.eventId = eventId;
		this.time = time;
		this.cellId = cellId;
	}
    
    public ValueData(String eventId, String time) {
    	this.eventId = eventId;
		this.time = time;
    }
	
	public String getEventId(){return eventId;}
	public String getTime(){return time;}
	public String getCellId(){return cellId;}
	
	public int getSeconds() {
		return getMinutes()*60;
	}
	
	public int getMinutes(){
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		Calendar cal = new GregorianCalendar();
		
		Date date = null;
		try {
			date = sdf.parse(time);
		} catch (ParseException e) {
			System.out.println("Expected format: HH:MM:SS.");
			System.out.println("Founded: " + time);
			System.out.println("Message: " + e.getMessage());
		}
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
		
		vd1Seconds = this.getSeconds();
		vd2Seconds = vd2.getSeconds();
		
		
		if(vd1Seconds < vd2Seconds) {
			return -1;
		} else if(vd1Seconds == vd2Seconds) {
			return 0;
		} else {
			return 1;	
		}
		
	}

}

