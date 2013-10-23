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


public class ValueData implements Writable {

	private String eventId;
	private String time;
	public ValueData(){}
	
	public ValueData(String eventId, String time) throws ParseException{
		
		this.eventId = eventId;
		this.time = time;
	}
	
	
	public String getEventId(){return eventId;}
	public String getTime(){return time;}
	public int getSeconds() throws ParseException{
		
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		Calendar cal = new GregorianCalendar();
		Date date = sdf.parse(time);
		cal.setTime(date);

		return cal.get(Calendar.SECOND) + cal.get(Calendar.MINUTE)*60 + cal.get(Calendar.HOUR_OF_DAY) * 60*60;
	}

	public void readFields(DataInput in) throws IOException {
		eventId = in.readUTF();
		time = in.readUTF();				
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(eventId);
		out.writeUTF(time);
		
	}
	
	public static ValueData read(DataInput in) throws IOException {
		ValueData vd = new ValueData();
		vd.readFields(in);
		return vd;
		
	}
	
	public int compare(ValueData vd1, ValueData vd2){
		
		int vd1Minutes = 0;
		int vd2Minutes = 0;
		
		try{
			vd1Minutes = vd1.getSeconds();
			vd2Minutes = vd2.getSeconds();
		}catch(ParseException e){
			
			System.out.println(e.getMessage());
			
		}
		return vd1Minutes < vd2Minutes ? -1  : vd1Minutes == vd2Minutes ? 0 : 1;
		
	}
	
	

}

