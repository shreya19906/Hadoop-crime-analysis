package CrimeData;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearCrimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
        String line = value.toString();
        String[] recs = line.split(",");
        if(!recs[0].equalsIgnoreCase("ID")){
        	String year = recs[1];
        	context.write(new Text(year), new IntWritable(1));
        }
	}

}
