import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Driver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		  Job job =Job.getInstance(conf, "CovidvsGDP");
		  Path Covid19Inp = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/Covid-19-SG-2020.tsv");
		  Path QuatStatInp = new Path("hdfs://localhost:9000/user/phamvanvung/project/input/QuaStat.tsv");
		  MultipleInputs.addInputPath(job, Covid19Inp, TextInputFormat.class, CovidMapper.class);
		  MultipleInputs.addInputPath(job, QuatStatInp, TextInputFormat.class, GDPMapper.class);
		  
		  job.setJarByClass(Driver.class);
		  
		  job.setReducerClass(DataReducer.class);
		  
		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  
		  
		  
		  Path outPath = new Path("hdfs://localhost:9000/user/phamvanvung/project/output");
		  outPath.getFileSystem(conf).delete(outPath, true);
		  FileOutputFormat.setOutputPath(job, outPath);
		  System.exit(job.waitForCompletion(true)?0:1);

	}

}

