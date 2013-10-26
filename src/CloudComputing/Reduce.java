package CloudComputing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
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
	//so para commit

	
	public void reduce(KeyData key, Iterator<ValueData> value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		//if its a network event
		//ligacao a BD
		//List<ValueData> list = new ArrayList<ValueData>();
		//while(value.hasNext())
		//{
			//list.add(value.next());
		//}
		//Collections.sort(list);
		
		Configuration conf =  HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		String family = key.getTypeDistinguisher();
		
		if(key.getTypeDistinguisher().equals("VC")){
			
			HTable table = new HTable(conf,"Cell");
			Put put = new Put(Bytes.toBytes(key.getPhoneId() +"_" +key.getDate()));
			
			
			String cellSequence = getCellSequence(value);
			
    		put.add(Bytes.toBytes(key.getTypeDistinguisher()), Bytes.toBytes("cellSequence"), Bytes.toBytes(cellSequence));
    		
    		table.put(put);


		} else if (key.getTypeDistinguisher().equals("MO")){
			
			HTable table = new HTable(conf,"Cell");
			Put put = new Put(Bytes.toBytes(key.getPhoneId() +"_" +key.getDate()));
    		put.add(Bytes.toBytes(key.getTypeDistinguisher()), Bytes.toBytes("secondsOff"), Bytes.toBytes(String.valueOf(getSecondsOff(value))));
    		table.put(put);
			
			
		}
		else {
			
			//List<Integer> list = getListOfHoursPresent(value);
			//String concat = "";
			//for(Integer intValue: list)
				//concat += (intValue.valueOf(intValue) + " ");
			
			//HTable table = new HTable(conf,"phonePresence");
			//Put put = new Put(Bytes.toBytes(key.getPhoneId() + "_" + key.getDate() + "_" + key.getCellId()));
    		//put.add(Bytes.toBytes("PP"), Bytes.toBytes("presentHour"), Bytes.toBytes(concat));
    		//table.put(put);
			HTable table = new HTable(conf,"phonePresence");
			Put put = new Put(Bytes.toBytes(key.getCellId() + "_" + key.getDate() + "_" + horasemponto??));
    		put.add(Bytes.toBytes("PP"), Bytes.toBytes("phonesPresent"), Bytes.toBytes(concat));
    		table.put(put);
			
		}
		
		admin.close();

	}
	
	public String getCellSequence(Iterator<ValueData> valuesList){
		
		StringBuilder sequence = new StringBuilder();
		ValueData vd;
		
		while(valuesList.hasNext()){
			
			vd = valuesList.next();
			sequence.append(vd.getCellId());
			sequence.append(" ");
		
		}
		
		return sequence.toString();
		
	}
	
	public List<Integer> getListOfHoursPresent(Iterator<ValueData> valuesList) {
		
		
		ValueData vd;
		
		int hour1 = 0;
		int hour2 = 0;
		
		boolean hasProcessedOne = false;
		boolean hasProcessedTwo = false;
		int firstEvent = 0;
		
		int i = 0;
		int z = 0;
		int temp;
		
		List<Integer> presentInstants = new ArrayList<Integer>();
				
		
		if(!valuesList.hasNext()) {
			return null;
		}
		
		
		while(valuesList.hasNext()) {
			
			vd = valuesList.next();
			
			if(vd.getEventId().equals("2")){
				
				try {
					
//					if(hasProcessedOne) {
//						hasProcessedTwo = true;
//					}
					
					if(!hasProcessedOne) {
						hasProcessedOne = true;
						firstEvent = 2;
					}
					
					hour1 = vd.getSeconds();

					
				} catch (ParseException e) {
					e.printStackTrace();
				}
			
			
			} else if(vd.getEventId().equals("3")) {
				
				try {
					
					
					hour2 = vd.getSeconds();
					
					if(hasProcessedOne) {					
						hasProcessedTwo = true;
					}
					
					if(!hasProcessedOne) {
						addElementTocreateList(presentInstants, 0, hour2);
						hasProcessedOne = false;
						
						continue;
						//firstEvent = 3; 
					}
					
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
			}

			if(hasProcessedTwo) {

				addElementTocreateList(presentInstants, hour1, hour2);
				hasProcessedOne = false;
				hasProcessedTwo = false;
				
			} else {
				
				continue;
				
			}
			
		}
		
//		if(hasProcessedOne) {
//			hour2 = 24*60*60 - 1;
//			addElementTocreateList(presentInstants, hour1, hour2);
//		}

		return presentInstants;
		
	}
	
	private void addElementTocreateList(List<Integer> presentInstants, int time1, int time2) {
		
		int z = 1;
		int temp1 = 0;
		int temp2 = 0;
		
		temp1 = time1;
		temp2 = time2;
		
		while(temp1 <= temp2) {
			if((temp1 % (60*60)) == 0){
				z = 60*60; 
				presentInstants.add(temp1);
				
			}
			temp1 += z;
		}		
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
	
		