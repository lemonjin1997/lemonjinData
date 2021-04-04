//Contributor: Li Qi, Shi En
import java.io.IOException; 
import java.util.Map; 
import java.util.TreeMap; 

import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context; 
public class top10_Reducer extends Reducer<Text, 
LongWritable,Text,LongWritable> { 

	private TreeMap<Long, String> tmap2; 
	@Override
	public void setup(Context context) throws IOException, 
	InterruptedException 
	{ 
		tmap2 = new TreeMap<Long, String>(); 
	} 
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, 
			Context context) throws IOException, InterruptedException 
	{ 
		String name = key.toString(); 
		long count = 0; 

		for (LongWritable val : values) 
		{ 
			count = val.get(); 
		} 
		// we want top 10 cumulative death, so we pass count as key 
		tmap2.put(count, name); 
		// we remove the first key-value , if it's size increases 10 
		if (tmap2.size() > 10) 
		{ 
			tmap2.remove(tmap2.firstKey()); 
		} 
	} 
	@Override
	public void cleanup(Context context) throws IOException, 
	InterruptedException 
	{ 
		for (Map.Entry<Long, String> entry : tmap2.entrySet())  
		{ 

			long count = entry.getKey(); 
			String name = entry.getValue(); 

			context.write(new Text(name), new LongWritable(count)); 
		} 
	} 
} 