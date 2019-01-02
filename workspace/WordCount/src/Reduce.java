import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
    IntWritable outputValue =new IntWritable();
    
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		for (IntWritable val : values) {
			if (val.get() != 1) throw new RuntimeException("fail!");
			sum += val.get();
		}
		
		outputValue.set(sum);
		context.write(key,outputValue);
	}
}
