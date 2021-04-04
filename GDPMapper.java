//Contributor: Wei Ren, Bryan
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GDPMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static final String file = "Quaterly\t";
	
	protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,Text>.Context context)
		throws IOException, InterruptedException {
		//System.out.println("GDPMapper");
		
		String GDP = value.toString();
		String[] parts= GDP.split("\t");
		String[] Quarter = parts[0].split(" ");
		String allVal = parts[1] + "\t" + Quarter[0];
		//System.out.println(Quarter[0] + "\t" + parts[1]);
		context.write(new Text(Quarter[0]), new Text(file + allVal));
	}

}