package invertedIndex;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.io.IOException;

public class RetrievalMapper extends Mapper<LongWritable, Text, ScoreFileIdPair, QueryAnswer> {
	private ScoreFileIdPair outputKey = new ScoreFileIdPair();
	private QueryAnswer outputValue = new QueryAnswer();
	private String[] queries;
	private int totalFileCount;
	private RowData lineData;
	private boolean ignoreCase;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		FileNameAndIdConvertor convertor = new FileNameAndIdConvertor(context);
		totalFileCount = convertor.getTotalFileCount();
		queries = context.getConfiguration().get(InvertedIndexSetting.CONF_QUERY_KEY,"").split("\\s+");
		ignoreCase = context.getConfiguration().getBoolean(InvertedIndexSetting.IGNORE_LETTER_CASE,false);
		lineData = new RowData();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parseLineData(value.toString());
		if ( isMatchQuery(lineData.word) ){
			outputValue.setWord(lineData.word);
			for (FileSection fileSection : lineData.fileSections){
				double score = calculateScore(fileSection.offsets.size(), lineData.documentFrequency, totalFileCount);

				outputKey.setScore(score);
				outputKey.setFileId(fileSection.fileId);

				outputValue.setFileId(fileSection.fileId);
				outputValue.setScore(score);
				outputValue.clearOffsets();
				for ( Long offset : fileSection.offsets ){
					outputValue.addOffset(offset);
				}
				context.write(outputKey, outputValue);
			}
		}
	}

	private boolean isMatchQuery(String word){

		for ( String query : queries ){
			if ( (ignoreCase && query.toLowerCase().equals(word.toLowerCase())) || query.equals(word) ){
				return true;
			}
		}

		return false;
	}

	private static double calculateScore(int termFrequency, int documentFrequency, int totalFileCount){
		double score = 1.0*termFrequency*Math.log10(1.0*totalFileCount/documentFrequency);
		return score;
	}

	private void parseLineData(String line){
		String[] token = line.split("\\s+");
		int index;
		int termFrequency;

		lineData.word = token[0];
		lineData.documentFrequency = Integer.parseInt(token[1]);
		index = 2;
		lineData.fileSections.clear();
		while(index < token.length){
			FileSection fileSection = new FileSection();
			fileSection.fileId = Integer.parseInt(token[index++]);
			termFrequency = Integer.parseInt(token[index++]);
			for ( int i = 0 ; i < termFrequency; i++){
				fileSection.offsets.add(Long.parseLong(token[index++]));
			}
			lineData.fileSections.add(fileSection);
		}
	}

	private class RowData{
		public String word;
		public int documentFrequency;
		public ArrayList<FileSection> fileSections = new ArrayList<FileSection>();
	}

	private class FileSection{
		public int fileId;
		public ArrayList<Long> offsets;

		public FileSection(){
			offsets = new ArrayList<Long>();
		}
	}

}
