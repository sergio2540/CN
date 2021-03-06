package CloudComputing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<KeyData, ValueData, LongWritable, Text> {

	private HBaseAdmin admin;
	//private HTable table;
	private RemoteHTable table;
	public void setup(String tableName) throws IOException{

		//54.194.23.170
		Configuration conf =  HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "54.194.23.170");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("hbase.regionserver.port", "60030");
		conf.set("hbase.regionserver.info.bindAddress", "54.194.23.170");
		
		this.admin = new HBaseAdmin(conf);
		//this.table = new HTable(conf, tableName);
		
		Cluster cluster = new Cluster();
		cluster.add("ec2-54-194-23-170.eu-west-1.compute.amazonaws.com", 8080);
		Client  client = new Client(cluster);
		this.table = new RemoteHTable(client, tableName);
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
				//putToTable(key.getPhoneId() + "_" + key.getDate() ,"VC", "cellSequence", cellSequence);
				appendToPhonePresence(key.getPhoneId() + "_" + key.getDate() ,"VC", "cellSequence", cellSequence); 
				cleanUp();
			} catch (Exception e) {
				System.out.println("Error while adding to table Cell, VC family");
				System.out.println("Message: " + e.getMessage());
			}

		} else if (typeDistinguisher.equals("MO")){


			try {
				setup("Cell");

				//(event4:time event5:time)
				//new String(data);
				StringBuilder string = new StringBuilder();
				
				for(ValueData vd : sortedVd) {
					string.append(vd.getSeconds());
					string.append(":");
					string.append(vd.getEventId());
					string.append(" ");
				}
				System.out.println("Passou o stringbuilder--------------------------------------");
				String vdOnString = string.toString();

				String row = key.getPhoneId() + "_" + key.getDate();
				Get get = new Get(Bytes.toBytes(row));
				boolean exists = false;
				Result r = null;
				System.out.println("Antes do try table exists-------------------------------------------");

				try {
					
					exists = table.exists(get);
					if(exists)
					 r = table.get(get);
					
				} catch (IOException e1) {
					System.out.println("Erro ao verificar existencia de entrada em minutes off: " + e1.getMessage());
				}

				byte[] data = null;
				String dataString = null;
				System.out.println("Antes do if exists--------------------------------------");

				if(exists){
					
					data = r.getValue(Bytes.toBytes("ES"), Bytes.toBytes("eventSequence"));
					System.out.println("Exists old minutsoff: .------------____>" + new String(data));
					dataString = new String(data);
					String orderedString = orderChronologically(dataString, vdOnString);
					
					System.out.println("orderedString---------------> " + orderedString);

					List<ValueData> vd = new ArrayList<ValueData>();
					
					ValueData valueData = null;
					
					String[] parseValueData = orderedString.split(" ");
					String[] parseValueDataFields;
					
					for(int i = 0; i < parseValueData.length; i++) {
		
						parseValueDataFields = parseValueData[i].split(":");
						
						if(parseValueDataFields[0] != null && parseValueDataFields[1] != null) {
							valueData = new ValueData(parseValueDataFields[1], formatIntoHHMMSS(Integer.parseInt(parseValueDataFields[0])));
							vd.add(valueData);
						} else {
							System.out.println("NULLLLLLLLLLLLLLLLLLLLLLLLLLLLL");
						}
						
					}
					
					putToTable(row, "MO", "minutesOff", String.valueOf(getMinutesOff(vd.iterator())));
					putToTable(row, "ES", "eventSequence", orderedString);
					
				} else {
					
					System.out.println("There are no minutes off!!!!!!!!!!!!!!!!!");
					putToTable(row, "MO", "minutesOff", String.valueOf(getMinutesOff(sortedVd.iterator())));
					putToTable(row, "ES", "eventSequence", vdOnString);
				
					//putToTable(String row, String family, String  qualifier, String value)
				}
				
				cleanUp();
				
			} catch (Exception e) {
				System.out.println("Error while adding to table Cell, MO family");
				System.out.println("Message: " + e.getMessage());
			}
		} else if (typeDistinguisher.equals("PP")) {
			Set<Integer> list = getListOfHoursPresent(sortedVdIterator);
			try {
				setup("phonePresence");
				appendToPhonePresence(list,key.getCellId() + "_" + key.getDate() + "_" ,"PP", "phoneList", key.getPhoneId() + " ");
				cleanUp();
			} catch (Exception e) {
				System.out.println("Error while adding to table phonePresence, PP family");
				System.out.println("Message: " + e.getMessage());
			}

		}
	}
	
	public String formatIntoHHMMSS(int secsIn)
	{

	int hours = secsIn / 3600,
	remainder = secsIn % 3600,
	minutes = remainder / 60,
	seconds = remainder % 60;

	return ( (hours < 10 ? "0" : "") + hours
	+ ":" + (minutes < 10 ? "0" : "") + minutes
	+ ":" + (seconds< 10 ? "0" : "") + seconds);

	} 

	public String getCellSequence(Iterator<ValueData> valuesList){

		StringBuilder sequence = new StringBuilder();
		ValueData vd;

		while(valuesList.hasNext()) {
			vd = valuesList.next();
			sequence.append(String.valueOf(vd.getSeconds()));//--------------------
			sequence.append(":");//-----------------------------------------------
			sequence.append(vd.getCellId());
			sequence.append(" ");
		}

		return sequence.toString();
	}

	//Query 2
	public Set<Integer> getListOfHoursPresent(Iterator<ValueData> valuesList) {

		ValueData vd;

		int hour1 = 0;
		int hour2 = 0;

		boolean hasProcessedOne = false;
		boolean hasProcessedTwo = false;
		int firstEvent = 0;

		Set<Integer> presentInstants = new HashSet<Integer>();

		String eventId = null;


		if(!valuesList.hasNext()) {
			return null;
		}

		while(valuesList.hasNext()) {

			vd = valuesList.next();

			eventId = vd.getEventId();

			if(eventId.equals("8")){

				int hour = vd.getSeconds() / (60*60);
				presentInstants.add(hour);


			} else if(eventId.equals("2") || eventId.equals("4")){

				if(!hasProcessedOne) {
					hasProcessedOne = true;
					firstEvent = 2;
				}

				hour1 = vd.getSeconds();

			} else if(eventId.equals("3") || eventId.equals("5")) {



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
		if(hasProcessedOne) {
			hour2 = 24*60*60 - 1;
			addElementTocreateList(presentInstants, hour1, hour2);
		}

		return presentInstants;
	}

	private void addElementTocreateList(Set<Integer> presentInstants, int time1, int time2) {

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


	//Query 3

	public int getMinutesOff(Iterator<ValueData> valuesList) {
		return getSecondsOff(valuesList) / 60;
	}

	public int getSecondsOff(Iterator<ValueData> valuesList) {

		if(!valuesList.hasNext())
			return 0;

		int secondsOff = 0;
		int prevS = 0;
		int newS = 0;
		ValueData vd = null;

		while(valuesList.hasNext()){

			vd = valuesList.next();

			newS = vd.getSeconds();

			if(vd.getEventId().equals("4")) {
				secondsOff += (newS - prevS);
				prevS = newS;			
			} else if (vd.getEventId().equals("5")) {
				prevS = newS;
			} else if (vd.getEventId().equals("8"))  {
				continue;
			}
		}

		if(vd.getEventId().equals("5")) {
			secondsOff += 86400 - prevS;
		}

		return secondsOff;

	}


	public void putToTable(String row, String family, String  qualifier, String value) throws Exception{

		System.out.println("Value:" + value);
		Put put = new Put(row.getBytes());
		put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
		this.table.put(put);	

	}


	//public void appendToPhonePresence(Collection<Integer> list ,String row, String family, String  qualifier, String value) throws Exception{

	//List<Append> batch = new ArrayList<Append>();

	//for(Integer temp : list)
	//{
	//Append append = new Append((row + String.valueOf(temp)).getBytes());
	//append.add(family.getBytes(), qualifier.getBytes(),(value + " ").getBytes());
	//this.table.append(append);
	//batch.add(append);
	//}
	//this.table.batch(batch);
	//check if null
	//}

	//}


	public void appendToPhonePresence(Collection<Integer> list ,String row, String family, String  qualifier, String value) throws Exception{

		for(Integer temp : list)
		{
			try	
			{
				String rowId = row +  String.valueOf(temp);
				Result r = null;
				try {
					Get get = new Get(Bytes.toBytes(rowId));
					r = table.get(get);
				} catch(Exception e){
					System.out.println(e.getMessage());
				}

				byte[] data = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				String stringedData = "";
				if(data != null){
					stringedData = new String(data);
					stringedData += value;
					//stringedData = orderChronologically(new String(data), value);
					//System.out.println("------------------------------>Segunda vez: " + stringedData);
					putToTable(rowId , family, qualifier, stringedData);

				} else {
					//System.out.println("------------------------->Primeira vez: " + value);
					putToTable(rowId , family, qualifier, value);
				}

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		}

		//this.table.batch(batch);
		//check if null
	}

	public String orderChronologically(String onDB, String toDB){

		String[] onDBbySpace = onDB.split(" ");
		String[] toDBbySpace = toDB.split(" ");
		StringBuilder orderedString = new StringBuilder();

		TreeMap<Integer,String> ordered = new TreeMap<Integer,String>();

		for(String onDBtemp : onDBbySpace){

			String[] onDBbyDots = onDBtemp.split(":");
			ordered.put(Integer.parseInt(onDBbyDots[0]), onDBbyDots[1]);
		}


		for(String toDBtemp : toDBbySpace){

			String[] toDBbyDots = toDBtemp.split(":");
			ordered.put(Integer.parseInt(toDBbyDots[0]), toDBbyDots[1]);
		}

		Set<Entry<Integer,String>> orderedSet = ordered.entrySet();

		for(Entry entry : orderedSet){

			orderedString.append(entry.getKey().toString());
			orderedString.append(":");
			orderedString.append(entry.getValue());
			orderedString.append(" ");

			
			

		}

		return orderedString.toString();

		/*int value1 = 0;
		int value2 = 1;
		int onDBLength = onDBbySpace.length;

		for(;value2 < onDBLength;){
			String lowerBoundS = onDBbySpace[value1].split(":")[0];
			String upperBoundS = onDBbySpace[value2].split(":")[0];
			int lowerBoundI = Integer.parseInt(lowerBoundS);
			int upperBoundI = Integer.parseInt(upperBoundS);

			for(String toDBString : toDBbySpace){

				String toDBBydots = toDBString.split(":")[0];
				int toDBSeconds = Integer.parseInt(toDBBydots);
				if(toDBSeconds > lo)


			}
			}
		 */	



	}

	public void appendToPhonePresence(String row, String family, String  qualifier, String value){

		boolean exists = true;

		String rowId = row;
		Result r = null;
		Get get = new Get(Bytes.toBytes(rowId));
		try {

			if(exists = table.exists(get))
				r = table.get(get);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		byte[] data = null;
		if(exists){

			data = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		}

		String stringedData = null;
		try{
			if(data != null){
				//stringedData = new String(data);
				//stringedData += value;
				stringedData = orderChronologically(new String(data), value);
				System.out.println("------------------------------>Segunda vez: " + stringedData);
				putToTable(rowId , family, qualifier, stringedData);

			} else {
				System.out.println("------------------------->Primeira vez: " + value);
				putToTable(rowId , family, qualifier, value);
			}
		}catch(Exception e){

			System.out.println("Exception on putTotable! " + e.getMessage());
		}


	}

	//this.table.batch(batch);
	//check if null
	public void updateMinutesOff(String row, String family, String  qualifier, int value) throws Exception {
		
	//putToTable(key.getPhoneId() + "_" +key.getDate() ,"MO", "minutesOff", String.valueOf(getMinutesOff(sortedVdIterator)));
	
		Result r = null;
		boolean hasOldInfo= false;
	
		Get get = new Get(Bytes.toBytes(row));
	
		byte[] data = null;
	
		if(table.exists(get)) {
			r = table.get(get);
			data = r.getValue(Bytes.toBytes("MO"), Bytes.toBytes("minutesOff"));
			hasOldInfo = true;
		}
		String toSend = null;
		if(hasOldInfo) {
			System.out.println("---------------------->Has old info: " + data);
			System.out.println("---------------------->New info: " + value);
			toSend = String.valueOf(((Integer.parseInt(new String(data))) + value));
	
		} else {
			System.out.println("---------------------->Has no old info: " + value);
			toSend = String.valueOf(value);
		}
	
		putToTable(row ,family, qualifier, toSend);	
	
	}

}












