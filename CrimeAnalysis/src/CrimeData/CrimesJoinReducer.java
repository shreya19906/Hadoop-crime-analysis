package CrimeData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimesJoinReducer extends
		Reducer<Text, Text, Text, Text> {

	List<Text> lcrimes = new ArrayList<Text>();
	List<Text> lcommunity = new ArrayList<Text>();
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

    	//after shuffling and sorting a list of values for a key is generated so Iterable value is used in generics
    	
		lcrimes.clear();
		lcommunity.clear();
		
		for (Text t : values) {
			if(t.charAt(0) == 'A'){
				lcrimes.add(new Text(t.toString().substring(1)));
			}else if(t.charAt(0) == 'B'){
				lcommunity.add(new Text(t.toString().substring(1)));
			}
		}
		//inner join
		if(!lcrimes.isEmpty() && !lcommunity.isEmpty()){
			for(Text A : lcrimes){
				for(Text B : lcommunity){
					context.write(A, B);
				}
			}
		}
		
		/*left outer join
		for(Text A : lcrimes){
			//if lcommunity is not empty, join A and B
			if(!lcommunity .isEmpty()){
				for(Text B : lcommunity){
					context.write(A, B);
				}
			}else{
				//else, output A by itself
				context.write(A, null);
			}
		}*/

	}
}
