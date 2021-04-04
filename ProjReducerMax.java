//Contributor: Li Qi, Shi En
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ProjReducerMax extends Reducer<Text, Text, Text, Text> {
public Map<String, String> UnsortedMap = new HashMap<String, String>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String name = "";
		int count = 0;
		for(Text t: values){
			String parts[] = t.toString().split("\t");
			if(parts[0].equals("maxDeaths")){
				if(UnsortedMap.containsKey(key.toString())){
					if(Double.parseDouble(UnsortedMap.get(key.toString())) < Double.parseDouble(parts[1]) ){
						UnsortedMap.put(key.toString(), parts[1]);
					}
				}
				else{
					UnsortedMap.put(key.toString(), parts[1]);
				}
				
			}else 
			if (parts[0].equals("name")){
				name=parts[1];
			}
		}
}

	@SuppressWarnings({"rawtypes", "unchecked"})
	protected void cleanup(Reducer.Context context) throws IOException, InterruptedException{
		Map<String, String> sortedMap = new TreeMap<String,String>(UnsortedMap);
		for (Map.Entry<String, String> entry : sortedMap.entrySet()){
			String name = entry.getKey();
			String deathz = entry.getValue();
			context.write(new Text(name), new Text(deathz));
			System.out.println("ProjReducerMax: "+name +" "+ deathz);
		}
	}
}