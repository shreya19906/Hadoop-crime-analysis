package CrimeData;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DiplomaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
        String line = value.toString();
        String[] recs = line.split(",");
        if(!recs[0].equalsIgnoreCase("ID")){
        	String diploma = recs[15];
        	context.write(new Text(diploma), new IntWritable(1));
        }
	}

}

