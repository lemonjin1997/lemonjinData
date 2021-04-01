import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class ProjMapperMaxDeaths extends Mapper<Object, Text, Text, Text> {
	private static final String file = "maxDeaths\t";
	
	@Override 
	protected void map(Object key, Text value, Mapper<Object,Text,Text,Text>.Context context)
		throws IOException, InterruptedException{
		
		String death = value.toString();
		String[] parts = death.split(",");
		
		if(!parts[0].equals("Date_reported") && parts.length > 0){
			if (parts.length == 8){
				String country = parts[2];
				String deaths = parts[7];
				//System.out.println(country + deaths);
				if (country != null && deaths != null && !country.isEmpty()){
						context.write(new Text(country), new Text(file + deaths));
				}
		}
		}
}

	private String max(String string) {
		// TODO Auto-generated method stub
		return null;
	}

}