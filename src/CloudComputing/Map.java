package CloudComputing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, KeyData, ValueData> {

	public void map(LongWritable key, Text value, OutputCollector<KeyData, ValueData> output, Reporter reporter) throws IOException {
		
		String line = value.toString().trim();
		
		if(line.equals("")) {
			return;
		}
		
		String tokens[] = new String[5];
		int i = 0;
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		while (tokenizer.hasMoreTokens()) {
			tokens[i] = tokenizer.nextToken().trim();  
			i++;
		}
		
		String option = tokens[3].trim();
	
			//Join/Leave Network
			if(option.equals("4") || option.equals("5"))
			{						    //phoneId date cellId typeDistinguisher
				KeyData kd = new KeyData(tokens[4], tokens[1], "","MO"); //Minutes off
											//eventId time cellId
				ValueData vd = new ValueData(tokens[3], tokens[2],"");
				output.collect(kd, vd);
				
				            //Join/Leave cell
			} else if(option.equals("2") || option.equals("3"))
			
			{	
				if(option.equals("2")){	
					                   //phoneId date cellId typeDistinguisher
					KeyData kd = new KeyData(tokens[4], tokens[1], "", "VC"); //Visited Cells Family
					ValueData vd;
												//eventId time cellId
						vd = new ValueData("", tokens[2], tokens[0]);
						output.collect(kd, vd);
				}
				
				KeyData kd = new KeyData(tokens[4], tokens[1], tokens[0], "PP"); //Phone Presence Family
				 								//eventId time cellId 
				ValueData vd = new ValueData(tokens[3], tokens[2], "");
				output.collect(kd, vd);
			}
		 else {
			System.out.println("Event-id droped: " + value);
		}
	}
}
