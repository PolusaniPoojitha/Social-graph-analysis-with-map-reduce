package Assignment3.Question2;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;;

/**
 * Hello world!
 *
 */
public class CommonFollowersDriver extends Configured implements Tool
{
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Configuration(), new CommonFollowersDriver(), args);
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job jobTracker=new Job(getConf());
		jobTracker.setJarByClass(CommonFollowersDriver.class);
		jobTracker.setJobName("Driver");
		
		FileInputFormat.addInputPath(jobTracker, new Path(args[0]));
		//FileOutputFormat.setOutputPath(jobTracker,new Path(args[1]));
		Path tempOut = new Path("temp");
		SequenceFileOutputFormat.setOutputPath(jobTracker, tempOut);
		jobTracker.setOutputFormatClass(SequenceFileOutputFormat.class);
		jobTracker.setMapperClass(CommonFollowerMapper.class);
		jobTracker.setReducerClass(CommonFollowerReducer.class);	
		jobTracker.setOutputKeyClass(Text.class);
		jobTracker.setOutputValueClass(Text.class);
		
		jobTracker.waitForCompletion(true);
		
		Job job2 = new Job();
	    job2.setJarByClass(CommonFollowersDriver.class);
	    job2.setJobName("CommonFollower");

	    //The input of job2 is the output of job 1
	    job2.setInputFormatClass(SequenceFileInputFormat.class);
	    SequenceFileInputFormat.addInputPath(job2, tempOut);
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	    job2.setReducerClass(CommonFollowerReducer.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.waitForCompletion(true);
	    return(job2.waitForCompletion(true) ? 0 : 1);
		
	}
}
