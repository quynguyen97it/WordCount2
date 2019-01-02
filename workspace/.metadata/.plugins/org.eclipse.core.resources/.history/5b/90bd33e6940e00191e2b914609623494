import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    System.out.println("Found key "+ key.toString());
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		//This is like a select word, 1 from values
		//but map will also combine 1's from duplicates into a collection as the value of the map
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			
			context.write(word, one); 
		}
	}
}