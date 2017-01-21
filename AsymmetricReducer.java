package Assignment3.Question1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AsymmetricReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//int count=0;
		ArrayList<String> vals=new ArrayList<String>();
		for(Text value:values)
		{
			vals.add(value.toString());
		}
		if(vals.size()==1)
		{
			for(String val:vals)
			{
				context.write(new Text(val), new Text());
			}
		}
//		if(vals.size()==1)
//		{
//			for(String val:vals)
//			{
//				
//				break;
//			}
//		}

	}
}
