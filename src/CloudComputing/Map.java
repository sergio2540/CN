package CloudComputing;

import java.io.IOException;
import java.text.ParseException;
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
		
		int option = Integer.parseInt(tokens[3].trim());
		if((option >= 2) && (option <= 5)) {
			if(option == 4 || option == 5)
			{							//phoneId date cellId typeDistinguisher
				KeyData kd = new KeyData(tokens[4], tokens[1], "","MO"); //Minutes off
				ValueData vd;
				try {				//eventId time cellId
					vd = new ValueData(tokens[3], tokens[2],"");
					output.collect(kd, vd);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
			} else if (option == 2 || option == 3){ //admitir que a saida de rede implica saida da cell id
				
				if(option == 2){	
					//to get the visited cells
					//phoneId date cellId typeDistinguisher
					KeyData kd = new KeyData(tokens[4], tokens[1], "", "VC"); //Visited Cells Family
					ValueData vd;
					try {                //eventId time cellId
						vd = new ValueData("", tokens[2], tokens[0]);
						output.collect(kd, vd);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				
				KeyData kd = new KeyData(tokens[4], tokens[1], tokens[0], "PP");//Visited Cells Family
				ValueData vd;
				try { //eventId time cellId 
					vd = new ValueData(tokens[3], tokens[2], "");
					output.collect(kd, vd);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("event-id descartado: " + value);
		}
	}
}
