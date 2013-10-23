package CloudComputing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Reduce extends MapReduceBase implements Reducer<KeyData, ValueData, LongWritable, Text> {

//public class Reduce extends TableReducer<KeyData, ValueData, ImmutableBytesWritable> {

	public void reduce(KeyData key, Iterator<ValueData> value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key

		//if its a network event
	
		//ligacao a BD
		
		//String st = key.getPhoneId() + " |" + key.getDate() + " |" + key.getCellId() + " |";
		//SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		//if(vd.getEventId().equals("4"))
		//st += getMinutesOff(value);
		
		if(key.getCellId().equals("")){
			//inserir na tabela Network
			Configuration conf =  HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTable table = new HTable(conf,"PhoneTimeOff");
			Put put = new Put(Bytes.toBytes("row1"));
    		put.add(Bytes.toBytes("Family"), Bytes.toBytes("minutesOff"), Bytes.toBytes(String.valueOf(getMinutesOff(value))));
    		table.put(put);
    		System.out.println("It has been added biatch!!");
    		//context.write(null, put);
    		admin.close();
    		output.collect(new LongWritable(0), new Text(String.valueOf(getMinutesOff(value))));
    		System.out.println("It has been written biatch!!");


		}else //inserir na cell id
		
		System.out.println("Not implemented dumbfuck!!");
		//output.collect(new LongWritable(0), new Text(String.valueOf(getMinutesOff(value))));	
		
	}
	
	
	public int getMinutesOff(Iterator<ValueData> valuesList){
	
	if(!valuesList.hasNext())
		return 0;
	
	int minutesOff = 0;
	int prevM = 0;
	int newM = 0;
	int lastM = 1440;
	ValueData vd = null;
	while(valuesList.hasNext()){
		 
		vd = valuesList.next();

		try {
			newM = vd.getMinutes();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		if(vd.getEventId().equals("4")){
			minutesOff += (newM - prevM);
			prevM = newM;
			System.out.println("newM: "+newM);
			System.out.println("minutesOff: " + minutesOff);
			System.out.println("Prev: " +prevM);
			
		}
		else if(vd.getEventId().equals("5"))
		{
			
			prevM = newM;
			
		}
		else { 
			System.out.println("ERROR!!!!");
			return 0;
		}
		
	}	
		
		return minutesOff;
	}
}