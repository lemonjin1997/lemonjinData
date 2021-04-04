//Contributor: Wei Ren, Bryan
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, Text, Text, Text>{
	 	@Override
	    public void reduce(Text key, Iterable<Text> values,
	      Context context) throws IOException, InterruptedException
	    {
	 		int QCon = 0;
	 		String quarterly = "";
	 		for(Text t: values){
				String parts[] = t.toString().split("\t");
	 			//System.out.println(t);
				if(parts[0].equals("DailyConfirmed")){
					if (parts[2].equals("Q1")){
						QCon += Integer.parseInt(parts[1]);
					}
					if (parts[2].equals("Q2")){
						QCon += Integer.parseInt(parts[1]);
					}
					if (parts[2].equals("Q3")){
						QCon += Integer.parseInt(parts[1]);
					}
					else{
						QCon += Integer.parseInt(parts[1]);
					}
					
				}else if (parts[0].equals("Quaterly")){
					quarterly = parts[1];
				}
			}
	 		String result = String.valueOf(QCon) + "\t" + quarterly;
	 		//System.out.println(key);
	 		context.write(new Text(key), new Text(result));
	        
	    }
}