package invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class RetrievalReducer extends Reducer<ScoreFileIdPair, QueryAnswer, NullWritable, Text>{
	private FileNameAndIdConvertor convertor;
	private final static int topN = 10;
	private Text outputValue = new Text();
	private int rank;
	private int printedCount;
	private double previousScore;
	private StringBuilder searchingQuery = new StringBuilder();
	@Override
	public void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException{
		searchingQuery.setLength(0);
		convertor = new FileNameAndIdConvertor(context);
		rank = 0;
		printedCount = 0;
		previousScore = Double.MAX_VALUE;
		searchingQuery.append("Searching Query : " + context.getConfiguration().get(InvertedIndexSetting.CONF_QUERY_KEY,""));
		if (context.getConfiguration().getBoolean(InvertedIndexSetting.IGNORE_LETTER_CASE,false)){
			searchingQuery.append(" (Ignore letter case)");
		}
		outputValue.set(searchingQuery.toString());
		context.write(NullWritable.get(), outputValue);
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			while (context.nextKey()) {
				if ( printedCount < topN ){
					reduce(context.getCurrentKey(), context.getValues(), context);
				}
				else{
					break;
				}
			}
		} finally {
			cleanup(context);
		}
	}
	@Override
	public void reduce(ScoreFileIdPair key, Iterable<QueryAnswer> values, Context context) throws IOException, InterruptedException {

		for ( QueryAnswer queryAnswer : values){
			if ( printedCount < topN ){
				if ( queryAnswer.getScore() < previousScore ){
					previousScore = queryAnswer.getScore();
					rank = printedCount + 1;
				}
				outputValue.set(getPrintableAnswer(context, queryAnswer));
				context.write(NullWritable.get(), outputValue);
				printedCount += 1;
			}
			else{
				break;
			}
		}
	}

	private String getPrintableAnswer(Context context, QueryAnswer queryAnswer) throws IOException{
		String result = "";
		int fileId = queryAnswer.getFileId();
		String fileName = convertor.getFileName(fileId);
		result += String.format("Rank #%02d  [%02d]%s \t score = %f\n", rank, fileId, fileName, queryAnswer.getScore() );
		result += String.format("Target word = %s\n", queryAnswer.getWord() );
		for (Long offset : queryAnswer.getOffsets()){
			result += String.format("\t (%d) %s\n", offset, getWordContentInFile(context, fileName, offset));
		}
		result += "============================================\n\n";
		return result;
	}

	private String getWordContentInFile(Context context, String fileName, long offset) throws IOException{
		String result;
		Configuration conf = context.getConfiguration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path inputFile = new Path(conf.get(InvertedIndexSetting.INPUT_FILES_DIR)+"/"+fileName);
		BufferedReader reader = new BufferedReader(
			new InputStreamReader(fileSystem.open(inputFile))
		);
		reader.skip(offset);
		result = reader.readLine();
		return result;
	}
}
