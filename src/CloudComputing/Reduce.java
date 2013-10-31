package CloudComputing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
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

	private HBaseAdmin admin;
	private HTable table;
	
	public void setup(String tableName) throws IOException{
		
		Configuration conf =  HBaseConfiguration.create();
		this.admin = new HBaseAdmin(conf);
		this.table = new HTable(conf, tableName);
		
	}
	
	public void cleanUp() throws IOException{
		
		this.admin.close();
		this.table.close();
	}
	
	public void reduce(KeyData key, Iterator<ValueData> value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		
		List<ValueData> sortedVd = new ArrayList<ValueData>();

		while(value.hasNext()){
			
			ValueData temp = value.next();
			
			ValueData vd = new ValueData(temp.getEventId(), temp.getTime(), temp.getCellId());
			System.out.println("Family1: " + key.getTypeDistinguisher() + " Cell: " + vd.getCellId() + " Event: " + vd.getEventId() + " Time: " + vd.getTime());
			sortedVd.add(vd);
		}
		
		Collections.sort(sortedVd);
		Iterator<ValueData> sortedVdIterator = sortedVd.iterator();
		
		
		String typeDistinguisher = key.getTypeDistinguisher();
		//addToTable(String tableName, String row, String family, String  qualifier, String value)
		if(typeDistinguisher.equals("VC")){
			
			String cellSequence = getCellSequence(sortedVdIterator);			
				try {
					setup("Cell");
					putToTable(key.getPhoneId() +"_" +key.getDate() ,"VC", "cellSequence", cellSequence);
					cleanUp();
				} catch (Exception e) {
					System.out.println("Error while adding to table Cell, VC family");
					System.out.println("Message: " + e.getMessage());
				}

		} else if (typeDistinguisher.equals("MO")){

				
				try {
					setup("Cell");
					putToTable(key.getPhoneId() + "_" +key.getDate() ,"MO", "minutesOff", String.valueOf(getMinutesOff(sortedVdIterator)));
					cleanUp();
				} catch (Exception e) {
					System.out.println("Error while adding to table Cell, MO family");
					System.out.println("Message: " + e.getMessage());
				}
		} else if (typeDistinguisher.equals("PP")) {
			List<Integer> list = getListOfHoursPresent(sortedVdIterator);
					try {
						setup("phonePresence");
						appendToTable(list,key.getCellId() + "_" + key.getDate() + "_" ,"PP", "phoneList", key.getPhoneId() + " ");
						cleanUp();
					} catch (Exception e) {
						System.out.println("Error while adding to table phonePresence, PP family");
						System.out.println("Message: " + e.getMessage());
					}
			
		}
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
			
					if(!hasProcessedOne) {
						hasProcessedOne = true;
						firstEvent = 2;
					}
					
					hour1 = vd.getSeconds();

			} else if(vd.getEventId().equals("3")) {
				
				

					hour2 = vd.getSeconds();
					
					if(hasProcessedOne) {					
						hasProcessedTwo = true;
					}
					
					if(!hasProcessedOne) {
						addElementTocreateList(presentInstants, 0, hour2);
						hasProcessedOne = false;
						continue;
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
	
				newS = vd.getSeconds();
	
			if(vd.getEventId().equals("4")){
				secondsOff += (newS - prevS);
				prevS = newS;			
			} else if(vd.getEventId().equals("5")){
				prevS = newS;
			} else { 
				System.out.println("Seconds off deals only with events 4 or 5");
				return 0;
			}
		}	
		
		return secondsOff;
		
	}

	
	public void putToTable(String row, String family, String  qualifier, String value) throws Exception{
				
			Put put = new Put(row.getBytes());
			put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
			this.table.put(put);	

	}
	
	
public void appendToTable(List<Integer> list ,String row, String family, String  qualifier, String value) throws Exception{

			List<Append> batch = new ArrayList<Append>();

			for(Integer temp : list)
			{
				Append append = new Append((row + String.valueOf(temp)).getBytes());
				append.add(family.getBytes(), qualifier.getBytes(),(value + " ").getBytes());	
				batch.add(append);
			}
			this.table.batch(batch);
			//check if null
		
	}
	
}
	
		