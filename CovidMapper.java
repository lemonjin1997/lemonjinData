import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class COVIDMapper extends Mapper<LongWritable, Text, Text, Text> {
	int previous = 0;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
		String[] parts = value.toString().split(",");
		if(parts.length > 0){
			if(!parts[0].equals("Date")){
				int difference = Math.abs(previous - Integer.parseInt(parts[3]));
				previous = Integer.parseInt(parts[3]);
				//System.out.println("COVIDMapper\t" + parts[0] + "\t" + difference );
				context.write(new Text(parts[0]), new Text("COVID\t" + Integer.toString(difference)));
			}
		}
		
		
	}

}
