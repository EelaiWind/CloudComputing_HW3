package invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class FileSectionReducer extends Reducer<WordFileIdPositionPair,LongWritable,Text,Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	//private StringBuilder positionCollection = new StringBuilder();
	public void reduce(WordFileIdPositionPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		//positionCollection.setLength(0);
		outputKey.set(key.getWord());
		int termFrequency = 0;
		for ( LongWritable position : values ){
			termFrequency += 1;
			//positionCollection.append(" "+position);
		}
		//positionCollection.insert(0,key.getFileId() + " " + termFrequency);
		//positionCollection.append(";");
		outputValue.set(key.getFileId() + " " + termFrequency);
		context.write(outputKey, outputValue);
	}
}
