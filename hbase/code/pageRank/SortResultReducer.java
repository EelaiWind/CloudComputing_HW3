package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class SortResultReducer extends Reducer<TitleScorePair,NullWritable,Text,DoubleWritable> {
	private Text outputKey = new Text();
	private DoubleWritable outputValue = new DoubleWritable();

	@Override
	public void reduce(TitleScorePair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		outputKey.set(key.getTitle());
		outputValue.set(key.getScore());
		context.write(outputKey, outputValue);
	}
}
