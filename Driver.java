
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	public static void main(String[] args) throws Exception {
		Configuration conflemonjin = new Configuration();
		Job joblemonjin = Job.getInstance(conflemonjin, "AirlineNegativeSentiments");
		joblemonjin.setJarByClass(Driver.class);
		Path COVID_Cases = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/COVID-Cases.csv");
		Path STI_2020 = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/STI_2020.csv");
		Path STI_2021 = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/STI_2021.csv");
		Path outPath_lemonjin = new Path("hdfs://localhost:9000/user/phamvanvung/project/output/lemonjin");
		outPath_lemonjin.getFileSystem(conflemonjin).delete(outPath_lemonjin, true);
		
		/*Configuration validationConf = new Configuration(false);
		ChainMapper.addMapper(job, ANSValidationMapper.class, LongWritable.class,
				Text.class, LongWritable.class, Text.class, validationConf);
		
		Configuration ansConf = new Configuration(false);
		ChainMapper.addMapper(job, ANSMapper.class, LongWritable.class, Text.class,
				Text.class, IntWritable.class, ansConf);*/
		
		//job.setMapperClass(ChainMapper.class);
		
		MultipleInputs.addInputPath(joblemonjin, COVID_Cases, TextInputFormat.class, COVIDMapper.class);
		MultipleInputs.addInputPath(joblemonjin, STI_2020, TextInputFormat.class, STIRateMapper.class);
		//MultipleInputs.addInputPath(job, STI_2021, TextInputFormat.class, STIRateMapper.class);
		
		//job.setCombinerClass(ANSReducer.class);
		joblemonjin.setNumReduceTasks(1);
		joblemonjin.setReducerClass(STICOVIDReducer.class);
		
		joblemonjin.setOutputKeyClass(Text.class);
		joblemonjin.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(joblemonjin, outPath_lemonjin);
		
		
		Configuration confBW = new Configuration();
		  Job jobBW =Job.getInstance(confBW, "CovidvsGDP");
		  Path Covid19Inp = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/Covid-19-SG-2020.tsv");
		  Path QuatStatInp = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/QuaStat.tsv");
		  MultipleInputs.addInputPath(jobBW, Covid19Inp, TextInputFormat.class, CovidMappers.class);
		  MultipleInputs.addInputPath(jobBW, QuatStatInp, TextInputFormat.class, GDPMapper.class);
		  
		  jobBW.setJarByClass(Driver.class);
		  
		  jobBW.setReducerClass(DataReducer.class);
		  
		  
		  jobBW.setOutputKeyClass(Text.class);
		  jobBW.setOutputValueClass(Text.class);
		  
		  
		  
		  Path outPathBW = new Path("hdfs://localhost:9000/user/phamvanvung/project/output/BW");
		  outPathBW.getFileSystem(confBW).delete(outPathBW, true);
		  FileOutputFormat.setOutputPath(jobBW, outPathBW);
		  
		  jobBW.waitForCompletion(true);
		  
		  joblemonjin.waitForCompletion(true);
	}

}
