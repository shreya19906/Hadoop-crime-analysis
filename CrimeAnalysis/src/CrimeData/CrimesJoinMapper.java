package CrimeData;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimesJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String outkey;
		String outvalue;
		//split the entire line on the basis of comma and feed the fields in the recs array
		String[] recs = value.toString().split(",");
		
		if (!recs[0].equals("ID")) {
			outkey = String.valueOf(recs[13]);// community id
			
			//StringBuilder classes are provided to modify the Strings 
			/*StringBuffer or Builder is mutable, its String representation isn't. 
			 * In Java, all strings are immutable(due to security issues). When you are trying to modify a String ,
			 *  what you are really doing is creating a new one. However, when you use a StringBuilder
			 *   , you are actually modifying the contents, instead of creating a new one*/
			
			StringBuilder sb = new StringBuilder();
			Date date = null;
			try {
				date = parseDate(recs[2], "MM/dd/yyyy hh:mm:ss a");
			} catch (ParseException e) {
				
				/*This is a checked exception an it can occur when you fail to parse a String
				 *  that is ought to have a special format. One very significant example on that 
				 *  is when you are trying to parse a String to a Date Object*/
				
				e.printStackTrace();
				return;
			}
			
			//storing the required attributes of data in the string to append and give it to reducer
			
			String id = recs[0];
			String year = recs[17];
			String month = getMonth(date);
			String day = getDay(date);
			String time = getTime(date);
			String timeperiod = getTimePeriod(date);
			String type = recs[5];
			String arrest = recs[8];
			String domestic = recs[9];
			if(year == null || year.length() != 4) return;
			
			sb.append(id).append(",").append(year).append(",").append(month).append(",").append(day)
					.append(",").append(time).append(",").append(timeperiod)
					.append(",").append(type).append(",").append(arrest).append(",")
					.append(domestic).append(",");
			
			outvalue = "A" + sb.toString();
			context.write(new Text(outkey), new Text(outvalue));
		}

	}
	private Date parseDate(String dateString, String sPattern)
			throws ParseException {

		SimpleDateFormat sf = new SimpleDateFormat(sPattern);
		Date date = sf.parse(dateString);
		return date;
	}

	private String getMonth(Date date) {
		//substring method returns the string in between the start and till end-1
		
		return new SimpleDateFormat("yyyyMMddHHmmss").format(date).substring(4,
				6);
	}

	private String getDay(Date date) {
		return new SimpleDateFormat("yyyyMMddHHmmss").format(date).substring(6,
				8);
	}

	private String getTime(Date date) {
		return new SimpleDateFormat("yyyyMMddHHmmss").format(date).substring(8,
				14);
	}

	private String getTimePeriod(Date date) {
		
		/*Calendar class in Java is an abstract class that provides methods for converting date
		 *  between a specific instant in time and a set of calendar fields such as MONTH, YEAR,
		 *   HOUR, etc. It inherits Object class and implements the Comparable, Serializable, Cloneable interfaces.*/
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour > 0 && hour <= 4) {
			return "0:00 - 4:00 AM";
		} else if (hour > 4 && hour <= 8) {
			return "4:00 - 8:00 AM";
		} else if (hour > 8 && hour <= 12) {
			return "8:00 - 12:00 AM";
		} else if (hour > 12 && hour <= 16) {
			return "12:00 - 4:00 PM";
		} else if (hour > 16 && hour <= 20) {
			return "4:00 - 8:00 PM";
		} else {
			return "8:00 - 12:00 PM";
		}
	}

}
