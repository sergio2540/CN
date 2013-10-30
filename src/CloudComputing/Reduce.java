package CloudComputing;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<KeyData, ValueData, LongWritable, Text> {

	public void reduce(KeyData key, Iterator<ValueData> value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		
		List<ValueData> sortedVd = new ArrayList<ValueData>();

		while(value.hasNext()){
			
			ValueData temp = value.next();
			
			ValueData vd;
			try {
				
				vd = new ValueData(temp.getEventId(), temp.getTime(), temp.getCellId());
				System.out.println("Family1: " + key.getTypeDistinguisher() + " Cell: " + vd.getCellId() + " Event: " + vd.getEventId() + " Time: " + vd.getTime());
				sortedVd.add(vd);
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		Collections.sort(sortedVd);
		
		for(ValueData _vd : sortedVd) {
			System.out.println("Family2: " + key.getTypeDistinguisher() + " Cell: " +_vd.getCellId() + " Event: " +_vd.getEventId() + " Time: " +_vd.getTime());
		}
		
		Iterator<ValueData> sortedVdIterator = sortedVd.iterator();
		
		Configuration conf =  HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		String typeDistinguisher = key.getTypeDistinguisher();
		
		if(typeDistinguisher.equals("VC")){
			
			HTable table = new HTable(conf,"Cell");
			Put put = new Put(Bytes.toBytes(key.getPhoneId() +"_" +key.getDate()));
			String cellSequence = getCellSequence(sortedVdIterator);
    		put.add(Bytes.toBytes(key.getTypeDistinguisher()), Bytes.toBytes("cellSequence"), Bytes.toBytes(cellSequence));
    		table.put(put);
    		table.close();

		} else if (typeDistinguisher.equals("MO")){
			
			HTable table = new HTable(conf,"Cell");
			Put put = new Put(Bytes.toBytes(key.getPhoneId() +"_" +key.getDate()));
    		put.add(Bytes.toBytes(key.getTypeDistinguisher()), Bytes.toBytes("minutesOff"), Bytes.toBytes(String.valueOf(getMinutesOff(sortedVdIterator))));
    		table.put(put);
    		table.close();
			
		} else if (typeDistinguisher.equals("PP")) {
			
			HTable table = new HTable(conf,"phonePresence");
			List<Integer> list = getListOfHoursPresent(sortedVdIterator);
			for(Integer intValue: list){
				Append append = new Append((key.getCellId() + "_" + key.getDate() + "_" + intValue.toString()).getBytes());
				append.add("PP".getBytes(), "phoneList".getBytes(),(key.getPhoneId() + " ").getBytes());
				table.append(append);
			}
			table.close();
		}
		admin.close();
	}
	
	public String getCellSequence(Iterator<ValueData> valuesList){
		
		StringBuilder sequence = new StringBuilder();
		ValueData vd;

		while(valuesList.hasNext()) {
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
		List<Integer> presentInstants = new ArrayList<Integer>();
				
		if(!valuesList.hasNext()) {
			return null;
		}
		
		while(valuesList.hasNext()) {
			
			vd = valuesList.next();
			if(vd.getEventId().equals("2")){
				
				try {
					
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
		
// Caso se queira contar ate as 24h
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
				presentInstants.add(temp1/(60*60));
			}
			temp1 += z;
		}		
	}
	
	public int getMinutesOff(Iterator<ValueData> valuesList) {
		return getSecondsOff(valuesList) / 60;
	}
	
	public int getSecondsOff(Iterator<ValueData> valuesList){

		if(!valuesList.hasNext())
			return 0;
		
		int secondsOff = 0;
		int prevS = 0;
		int newS = 0;
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
			} else if(vd.getEventId().equals("5")){
				prevS = newS;
			} else { 
				System.out.println("ERROR!!!!");
				return 0;
			}
		}	
		
		return secondsOff;
		
	}

}
	
		