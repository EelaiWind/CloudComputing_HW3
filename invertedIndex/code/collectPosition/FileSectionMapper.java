package invertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileSectionMapper extends Mapper<LongWritable, Text, WordFileIdPositionPair, LongWritable> {
	private FileNameAndIdConvertor convertor;
	private WordFileIdPositionPair outputKey = new WordFileIdPositionPair();
	private LongWritable outputValue = new LongWritable();
	final static private Pattern wordPattern = Pattern.compile("[a-zA-Z]+");
	final static private Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
	final static private Pattern textPattern = Pattern.compile("<text.*?>([\\S\\s]+?)</text>");
    final static private Pattern noTextPattern = Pattern.compile("<text.*?/>");
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		convertor = new FileNameAndIdConvertor(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Matcher titleMatcher = titlePattern.matcher(line);
		Matcher textMatcher = textPattern.matcher(line);

		if ( !titleMatcher.find()){
			throw new IOException("InvertedIndex : Input line doesn't have a title");
		}

		if ( textMatcher.find() ){
			String title = titleMatcher.group(1);
			String text = textMatcher.group(1);
			title = replaceSpecialString(title);
			title = capitalizeFirstLetter(title);

			Matcher matcher = wordPattern.matcher(text);
			int fileId = convertor.getFileId(title);
			while ( matcher.find() ){
				String word = matcher.group();
				int position = matcher.start();
				outputKey.set(word, fileId, position);
				outputValue.set(position);
				context.write(outputKey, outputValue);
			}
		}
		else{
			if ( !noTextPattern.matcher(line).find() ){
				throw new IOException("InvertedIndex : Input line doesn't have a text");
			}
		}
	}

	private String replaceSpecialString(String input){
		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "'");
	}

	private String capitalizeFirstLetter(String input){
		char firstChar = input.charAt(0);
		if ( (firstChar >= 'a' && firstChar <='z') || (firstChar>= 'A' && firstChar <= 'Z') ){
			if ( input.length() == 1 ){
				return input.toUpperCase();
			}
			else{
				return input.substring(0, 1).toUpperCase() + input.substring(1);
			}
		}
		else{
			return input;
		}
	}
}
