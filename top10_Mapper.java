//Contributor: Li Qi, Shi En
import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

public class top10_Mapper extends Mapper<Object, 
Text, Text, LongWritable> { 

	private TreeMap<Long, String> tmap; 

	@Override
	public void setup(Context context) throws IOException, 
	InterruptedException 
	{ 
		tmap = new TreeMap<Long, String>(); 
	} 

	@Override
	public void map(Object key, Text value, 
			Context context) throws IOException,  
	InterruptedException 
	{ 
		// input data format => country_name, cumulative_deaths  (tab seperated) we split the input data 
		String[] tokens = value.toString().split("\t"); 

		String country_name = tokens[0]; 
		long cumulative_deaths = Long.parseLong(tokens[1]); 

		// insert data into treeMap, we want top 10  country max death
		tmap.put(cumulative_deaths, country_name); 

		// we remove the first key-value, if it's size increases 10 
		if (tmap.size() > 10) 
		{ 
			tmap.remove(tmap.firstKey()); 
		} 
	} 
	@Override
	public void cleanup(Context context) throws IOException, 
	InterruptedException 
	{ 
		for (Map.Entry<Long, String> entry : tmap.entrySet())  
		{ 
			long count = entry.getKey(); 
			String name = entry.getValue(); 
			context.write(new Text(name), new LongWritable(count)); 
			System.out.println("Top10Mapper: "+name+" "+count);
		} 
	} 
} 
