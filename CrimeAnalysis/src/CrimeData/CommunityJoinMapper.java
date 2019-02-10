package CrimeData;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CommunityJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String outkey;
		String outvalue;
		String[] recs = value.toString().split(",");
		if (!recs[0].contains("Community")) {
			
			//community area number
			outkey = String.valueOf(recs[0]);
			
			//community area name
			String name = recs[1];
			
			//percent aged 16+ unemployed
			String un = recs[4];
			
			//percent aged 25+ without high school diploma
			String diploma = recs[5];
			
			//per capita income
			String income = recs[7];
			
			
			String unslot = unemployeed(recs[4]);//return the range of unemployees
			String diplomaslot = diploma(recs[5]);//return the range of people with diploma
			String incomeslot = income(recs[7]);//range of per capita income
			StringBuilder sb = new StringBuilder();
			sb.append(outkey).append(",").append(name).append(",").
				append(un).append(",").append(diploma).append(",").
				append(income).append(",").append(unslot).append(",").
				append(diplomaslot).append(",").append(incomeslot);
			outvalue = "B" + sb.toString();
			context.write(new Text(outkey), new Text(outvalue));
		}

	}
	
	private String unemployeed(String srate){
		float rate = Float.valueOf(srate);
		if(rate < 9) return "[4.7~9)";
		else if(rate >= 9 && rate < 15) return "[9~15)";
		else if(rate >= 15 && rate < 20) return "[15~20)";
		else return "[20~)";
	}
		
	private String diploma(String srate){
			float rate = Float.valueOf(srate);
			if(rate < 14) return "[2.5~14)";
			else if(rate >= 14 && rate < 20) return "[14~20)";
			else if(rate >= 20 && rate < 30) return "[20~30)";
			else return "[30~)";
	}
				
	private String income(String sincome){
					float income = Float.valueOf(sincome);
					if(income < 14000) return "[0~14000)";
					else if(income >= 14000 && income < 18000) return "[14000~18000)";
					else if(income >= 18000 && income < 24000) return "[18000~24000)";
					else if(income >= 24000 && income <35000) return "[24000~35000)";
					else return "[35000~)";
	}
}
