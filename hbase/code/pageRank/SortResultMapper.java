package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SortResultMapper extends Mapper<LongWritable, Text, TitleScorePair, NullWritable> {
	private TitleScorePair outputKey = new TitleScorePair();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] token = value.toString().split("\t",3);
		outputKey.setTitle(token[0]);
		outputKey.setScore(Double.parseDouble(token[1]));
		context.write(outputKey, NullWritable.get());
	}
}
