package CloudComputing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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

import CloudComputing.ValueData;


public class Reduce extends MapReduceBase implements Reducer<KeyData, ValueData, LongWritable, Text> {

//public class Reduce extends TableReducer<KeyData, ValueData, ImmutableBytesWritable> {

	public void reduce(KeyData key, Iterator<ValueData> value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		//if its a network event
	
		//ligacao a BD
		
		
		Configuration conf =  HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		
		if(key.getCellId().equals("")){
			HTable table = new HTable(conf,"PhoneTimeOff");
			Put put = new Put(Bytes.toBytes(key.getPhoneId() +"_" +key.getDate()));
    		put.add(Bytes.toBytes("NW"), Bytes.toBytes("secondsOff"), Bytes.toBytes(String.valueOf(getSecondsOff(value))));
    		table.put(put);


		}else //inserir na cell id
		
		
		admin.close();

	}
	
	
	//public int getMinutesOff(Iterator<ValueData> valuesList){
	public int getSecondsOff(Iterator<ValueData> valuesList){

	if(!valuesList.hasNext())
		return 0;
	
	int secondsOff = 0;
	int prevS = 0;
	int newS = 0;
	int lastS = 1440;
	ValueData vd = null;
	while(valuesList.hasNext()){
		 
		vd = valuesList.next();

		try {
			newS = vd.getSeconds();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		if(vd.getEventId().equals("4")){
			secondsOff += (newS - prevS);
			prevS = newS;
			System.out.println("newS: "+newS);
			System.out.println("secondsOff: " + secondsOff);
			System.out.println("Prev: " +prevS);
			
		}
		else if(vd.getEventId().equals("5"))
		{
			
			prevS = newS;
			
		}
		else { 
			System.out.println("ERROR!!!!");
			return 0;
		}
		
	}	
		
		return secondsOff;
	}
	

	
		
		
	
	
}
	
		