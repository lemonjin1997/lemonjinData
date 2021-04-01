import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ProjDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ProjDriver");
		Path countryInput = new Path("hdfs://localhost:9000/user/phamvanvung/proj/ISO-3166-alpha3.tsv");
		Path inputPath = new Path("hdfs://localhost:9000/user/phamvanvung/proj/input/WHO-COVID-19-global-data.csv");
		
		MultipleInputs.addInputPath(job, countryInput, TextInputFormat.class, ProjMapperCountry.class);
		MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, ProjMapperMaxDeaths.class);
		
		job.setJarByClass(ProjDriver.class);
		job.setReducerClass(ProjReducerTop10.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		Path outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/proj/output/" + new Date().getTime());
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
