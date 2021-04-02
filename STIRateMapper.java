//Contributor: Sabrina Lee Zhi Ying
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class STIRateMapper extends Mapper<LongWritable, Text, Text, Text> {
	double previous = 0.0;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
		String[] parts = value.toString().split(",");
		if(parts.length > 0){
		if(!parts[0].equals("Date")){
			String money = parts[1]+parts[2];
			money = money.replaceAll("\"", "");
			if (previous == 0){
				previous = Double.parseDouble(money);
			}
			else{
			int difference = (int)(previous - Double.parseDouble(money));
			previous = Double.parseDouble(money);
			
			String date = parts[0].replaceAll("/", "-");
			String dateParts[] =  date.split("-");
			//year month day
			String reformDate = dateParts[2] + "-" + dateParts[0] + "-" + dateParts[1];
			//System.out.println("STIRateMapper\t" + reformDate + "\t" + difference );
			if(!Integer.toString(difference).equals("0.0")){
			context.write(new Text(reformDate), new Text("STI\t" + Integer.toString(difference)));
			}
			}

		}
		}
	}
}
