import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProjMapperCountry extends Mapper<Object,Text,Text,Text>{
	private static final String file = "name\t";
	
	@Override
	protected void map(Object  key, Text value, Mapper<Object, Text,Text,Text>.Context context)
		throws IOException, InterruptedException {
		String countryRecord = value.toString();
		String[] parts= countryRecord.split("\t");
		//System.out.println(parts[0] + parts[1]);
		context.write(new Text(parts[1]), new Text(file + parts[0]));
	}
}