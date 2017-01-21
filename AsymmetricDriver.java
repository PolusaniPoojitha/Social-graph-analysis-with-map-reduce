package Assignment3.Question1;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class AsymmetricDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Configuration(), new AsymmetricDriver(), args);
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job jobTracker=new Job(getConf());
		jobTracker.setJarByClass(AsymmetricDriver.class);
		jobTracker.setJobName("AsymmetricDriver");
		
		FileInputFormat.addInputPath(jobTracker, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobTracker,new Path(args[1]));
		
		jobTracker.setMapperClass(AsymmetricMapper.class);
		jobTracker.setReducerClass(AsymmetricReducer.class);
//		
//		jobTracker.setMapOutputKeyClass(Text.class);
//		jobTracker.setMapOutputValueClass(Text.class);
//		
		jobTracker.setOutputKeyClass(Text.class);
		jobTracker.setOutputValueClass(Text.class);
		
		return jobTracker.waitForCompletion(true)?0:1;
	}

}
