import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class STICOVIDReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
		
		
		int COVID_difference = 0;
		double STI_difference = 0.0;
		for (Text value: values) {
			String line[] = value.toString().split("\t");
			if(line[0].equals("COVID")){
				COVID_difference = Integer.parseInt(line[1]);
			}
			else if(line[0].equals("STI")){
				STI_difference = Double.parseDouble(line[1]);
			}
			
		}
		if(STI_difference != 0.0){
			String difference = COVID_difference + "\t" + STI_difference;
			System.out.println(key+"\t"+difference);
			context.write(new Text(key), new Text(difference));
		}
		
	}
}
