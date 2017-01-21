package Assignment3.Question2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CommonFollowerMapper  extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String line=value.toString();
		String []users=line.split("\\s+");
		if(users.length>=2)
		{
			
			context.write(new Text(users[0]), new Text(users[1]));
		}
	}

}
