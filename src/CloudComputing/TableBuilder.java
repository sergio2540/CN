package CloudComputing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class TableBuilder {

	
	private HBaseAdmin admin;
	private String table1 = "Cell"; 
	private String table1VC = "VC";
	private String table1MO = "MO";
	private String table2 = "phonePresence";
	private String table2PP = "PP";

	
	public void cleanDB(){
		
		Configuration conf =  HBaseConfiguration.create();
		try {
			this.admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
		}
		try {
			this.admin.disableTable(table1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
		try {
			this.admin.deleteTable(table1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
		try {
			this.admin.disableTable(table2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
		try {
			this.admin.deleteTable(table2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
		

		
	}
	
	public void setDB() throws IOException{
		
		HTableDescriptor tab1Desc = new HTableDescriptor(table1);
		HColumnDescriptor tab1ColFamDesc1 = new HColumnDescriptor(table1VC);
		HColumnDescriptor tab1ColFamDesc2 = new HColumnDescriptor(table1MO);
		HTableDescriptor tab2Desc = new HTableDescriptor(table2);
		HColumnDescriptor tab2ColFamDesc1 = new HColumnDescriptor(table2PP);
		this.admin.createTable(tab1Desc);
		this.admin.disableTable(table1);
		this.admin.addColumn(table1, tab1ColFamDesc1);
		this.admin.addColumn(table1, tab1ColFamDesc2);
		this.admin.enableTable(table1);
		this.admin.createTable(tab2Desc);
		this.admin.disableTable(table2);
		this.admin.addColumn(table2, tab2ColFamDesc1);
		this.admin.enableTable(table2);
	}
	
}
