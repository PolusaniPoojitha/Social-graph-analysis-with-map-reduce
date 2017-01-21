package Assignment3.Question1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AsymmetricMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String line=value.toString();
		String []users=line.split("\t");
		if(users.length>=2)
		{
			int UserID=Integer.parseInt(users[0]);
			int FollowerId=Integer.parseInt(users[1]);
			if(UserID>FollowerId)
			{
				int temp=UserID;
				UserID=FollowerId;
				FollowerId=temp;
			}
			context.write(new Text(Integer.toString(UserID)+" "+Integer.toString(FollowerId)), new Text(users[0]+" "+users[1]));
		}
	}

}
