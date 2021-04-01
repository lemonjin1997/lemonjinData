import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CovidMappers extends Mapper<LongWritable, Text, Text, Text>{
		private static final String file = "DailyConfirmed\t";
			
			protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text,Text>.Context context)
				throws IOException, InterruptedException {
				//System.out.println("CovidMapper");
				String CovidRecords = value.toString();
				String[] parts= CovidRecords.split("\t");
				//System.out.println(parts[0] + "\t" + parts[1]);
				String quad = "";
				int count = 0;
				if(!parts[0].matches("Date")){
					String datePart[] = parts[0].split("-"); 
					if(datePart[1].equals("01") || datePart[1].equals("03") || datePart[1].equals("02")){
						quad = "Q1";
						count += Integer.parseInt(parts[1]);
					}
					if(datePart[1].equals("04") || datePart[1].equals("05") || datePart[1].equals("06")){
						quad = "Q2";
						count += Integer.parseInt(parts[1]);
					}
					if(datePart[1].equals("08") || datePart[1].equals("07") || datePart[1].equals("09")){
						quad = "Q3";
						count += Integer.parseInt(parts[1]);
					}
					if(datePart[1].equals("10") || datePart[1].equals("11") || datePart[1].equals("12")){
						quad = "Q4";
						count += Integer.parseInt(parts[1]);
					}
				}
				else{
					return;
				}
				//System.out.println(quad + "\t" + count);
				//System.out.println(quad + "\t" + parts[1]);
				String finalVal = count + "\t" + quad;
				//System.out.println(finalVal);
				context.write(new Text(quad), new Text(file + finalVal));
			}

}
