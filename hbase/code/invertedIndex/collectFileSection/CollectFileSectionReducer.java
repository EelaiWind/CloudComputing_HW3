package invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CollectFileSectionReducer extends Reducer<WordFileIdPair,Text,Text,Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private StringBuilder result = new StringBuilder();
	public void reduce(WordFileIdPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		result.setLength(0);
		int documentFrequency = 0;
		boolean isFirst = true;

		for (Text value : values){
			documentFrequency += 1;
			result.append(";"+value.toString());
		}

		outputKey.set(key.getWord());
		//result.insert(0,documentFrequency+";");
		outputValue.set(result.substring(1));
		context.write(outputKey, outputValue);
	}
}
