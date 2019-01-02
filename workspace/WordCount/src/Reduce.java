import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
    IntWritable outputValue =new IntWritable();
    
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		//this is a like group by with SUM, where the sum operates on all the 1's in the collection 
		for (IntWritable val : values) {
			if (val.get() != 1) throw new RuntimeException("fail!");
			sum += val.get(); //this is always 1 actually
			//sum += 1; 
		}
		
		// its interesting how this can work over a cluster!
		outputValue.set(sum);
		context.write(key,outputValue);
	}
}
