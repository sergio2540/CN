package CloudComputing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;



public class MainFilter {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		
		
		//JobConf conf = new JobConf(MainFilter.class);
		//conf.setJobName("Filter");
		Configuration config = HBaseConfiguration.create();
		JobConf conf = new JobConf(MainFilter.class);
		conf.setJobName("Filter");
		

		//conf.setOutputKeyClass(LongWritable.class);
		//conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(KeyData.class);
		conf.setMapOutputValueClass(ValueData.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		//TableMapReduceUtil.initTableReducerJob("PhoneTimeOff", Reduce.class, new Job(conf) );
		
	
		
		
	
		
	
	}
}
