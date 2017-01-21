package Assignment3.Question2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CommonFollowerReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//int count=0;
		
		StringBuilder listVals=new StringBuilder();
		ArrayList<Integer> vals=new ArrayList<Integer>();
		for(Text value:values)
		{
			vals.add(Integer.parseInt(value.toString()));
		}
		Collections.sort(vals);
		if(context.getJobName().equals("Driver"))
		{
			for(int i=0;i<vals.size();i++)
			{
				int j=i+1;
				while(j<vals.size())
				{
					context.write(new Text(vals.get(i)+"\t"+vals.get(j)), key);
				j++;
				}
			}
		}
		else if(context.getJobName().equals("CommonFollower"))
		{
			for(Integer val:vals)
			{
				listVals.append(val.toString()+",");
			}
			
			context.write(key, new Text("{"+listVals.substring(0,listVals.lastIndexOf(","))+"}"));
		}
	}

}
