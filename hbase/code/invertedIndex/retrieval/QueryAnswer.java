package invertedIndex;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;

public class QueryAnswer implements Writable{
	private Text word;
	private int fileId;
	private double score;
	private ArrayList<Long> offsets;

	public QueryAnswer(){
		word = new Text();
		offsets = new ArrayList<Long>();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		out.writeInt(fileId);
		out.writeDouble(score);
		out.writeInt(offsets.size());
		for(Long offset:offsets){
			out.writeLong(offset);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		fileId = in.readInt();
		score = in.readDouble();
		int offsetCount = in.readInt();
		offsets.clear();
		for (int i = 0 ; i < offsetCount; i++){
			offsets.add(in.readLong());
		}
	}

	public void setWord(String word){
		this.word.set(word);
	}

	public void setFileId(int fileId){
		this.fileId = fileId;
	}

	public void setScore(double score){
		this.score = score;
	}

	public void clearOffsets(){
		offsets.clear();
	}

	public void addOffset(long offset){
		offsets.add(offset);
	}

	public String getWord(){
		return word.toString();
	}

	public int getFileId(){
		return fileId;
	}

	public double getScore(){
		return score;
	}

	public ArrayList<Long> getOffsets(){

		return new ArrayList<Long>(offsets);
	}
}