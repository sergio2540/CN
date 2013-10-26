package controllers;

import play.*;
import play.mvc.*;

import views.html.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
import java.io.IOException;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Application extends Controller {

    public static Result index() {

        return ok(index.render("CN"));
    
   }



    public static Result query1(String phoneID, String date) {
	
	String tableName = "Cell";

	Configuration config = HBaseConfiguration.create();

	try {
		
		//key phoneID + "_" + date
		String key = phoneID + "_" + date;		

		HTable table = new HTable(config, tableName);
	
		Get g = new Get(Bytes.toBytes(key));

		org.apache.hadoop.hbase.client.Result r = table.get(g);

		String family = "VC";
		String qualifier = "cellSequence";

		byte[] value = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
	
		String result = Bytes.toString(value);

		return ok(result);

	} catch(IOException e){

		return ok("Erro");
	
	}
	
    }

    public static Result query2(String cellID, String date, String time) {
        return ok("cellID " + cellID + "date: " + date  + "time" + time + "!");
    }
    
    public static Result query3(String phoneID, String date) {
	
	String tableName = "Cell";

	Configuration config = HBaseConfiguration.create();
	
	try {
		
		//key phoneID + "_" + date
		String key = phoneID + "_" + date;		

		HTable table = new HTable(config, tableName);
	

		Get g = new Get(Bytes.toBytes(key));

		org.apache.hadoop.hbase.client.Result r = table.get(g);
	
		// family = NW
		// qualifier = secondsOff
		String family = "MO";
		String qualifier = "minutesOff";

		byte[] value = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
	
		String result = Bytes.toString(value);

		return ok(result);

	} catch(IOException e){

		return ok("Erro");
	
	}	
        //return ok("phoneID " + phoneID + "date: "+ date +"!");
    }

}
