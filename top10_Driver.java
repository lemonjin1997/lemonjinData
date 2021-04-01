import java.util.Date;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 
  
public class top10_Driver { 
  
    public static void main(String[] args) throws Exception 
    { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "top 10"); 
        job.setJarByClass(top10_Driver.class); 
  
        job.setMapperClass(top10_Mapper.class); 
        job.setReducerClass(top10_Reducer.class); 
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
        Path inputPath1 = new Path("hdfs://localhost:9000/user/phamvanvung/proj/output/1617197394064/part-r-00000");
		Path outputPath1 = new Path("hdfs://localhost:9000/user/phamvanvung/proj/output2/" + new Date().getTime());
		
        FileInputFormat.addInputPath(job, inputPath1); 
        FileOutputFormat.setOutputPath(job, outputPath1); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 
public class top10_Driver {

}
