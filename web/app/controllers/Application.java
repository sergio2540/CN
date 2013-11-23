package controllers;

import play.*;
import play.mvc.*;

import views.html.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import java.io.IOException;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;

public class Application extends Controller {

	private static RemoteHTable table;

	private static void connect(String tableName) throws IOException{
		
		Cluster cluster = new Cluster();
		cluster.add("ec2-54-194-23-170.eu-west-1.compute.amazonaws.com", 8080);
		Client  client = new Client(cluster);
		table = new RemoteHTable(client, tableName);

	}

	public static Result index() {

		return ok(index.render("CN"));
	    
	}

    public static Result query1(String phoneID, String date) {
	
		try {
			String tableName = "Cell";	
			connect(tableName);

			String key = phoneID + "_" + date;
			Get g = new Get(Bytes.toBytes(key));
	
			org.apache.hadoop.hbase.client.Result r = table.get(g);
	
			String family = "VC";
			String qualifier = "cellSequence";
	
			byte[] value = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		
			String result = Bytes.toString(value);
	
			return ok(result);
	
		} catch(Exception e){
	
			return ok("Erro");
		
		}
	
    }

    public static Result query2(String cellID, String date, String time) {
    	
    	try {
		String tableName = "phonePresence";
    		connect(tableName);

    		String key = cellID + "_" + date + "_" + time;		
    		Get g = new Get(Bytes.toBytes(key));
    		org.apache.hadoop.hbase.client.Result r = table.get(g);
    	
    		String family = "PP";
    		String qualifier = "phoneList";

    		byte[] value = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    	
    		String result = Bytes.toString(value);

    		return ok(result);

    	} catch(Exception e){

    		return ok("Erro");
    	
    	}	
    }
    
    public static Result query3(String phoneID, String date) {
		
		try {
			String tableName = "Cell";
	    		connect(tableName);

			String key = phoneID + "_" + date;		
	
			Get g = new Get(Bytes.toBytes(key));
	
			org.apache.hadoop.hbase.client.Result r = table.get(g);
			
			if(r.isEmpty()){
				return ok("24");
			}
		
			String family = "MO";
			String qualifier = "minutesOff";
	
			byte[] value = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		
			String result = Bytes.toString(value);
	
			return ok(result);
	
		} catch(Exception e){
	
			return ok("Erro");
		
		}	
    }

}
