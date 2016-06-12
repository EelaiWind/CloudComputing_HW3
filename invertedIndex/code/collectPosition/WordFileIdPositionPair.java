package invertedIndex;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class WordFileIdPositionPair implements WritableComparable{
	private Text word = new Text();
	private int fileId;
	private long position;

	public WordFileIdPositionPair(){

	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		out.writeInt(fileId);
		out.writeLong(position);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		fileId = in.readInt();
		position = in.readLong();
	}

	@Override
	public int compareTo(Object another) {
		WordFileIdPositionPair obj = (WordFileIdPositionPair) another;

		int wordComparision = getWord().compareTo(obj.getWord());
		if ( wordComparision == 0 ){
			int fileIdComparision = Integer.compare(getFileId(), obj.getFileId());
			if ( fileIdComparision == 0){
				return Long.compare(getPosition(), obj.getPosition());
			}
			else{
				return fileIdComparision;
			}
		}
		else{
			return wordComparision;
		}
	}

	public String getWord(){
		return word.toString();
	}

	public int getFileId(){
		return fileId;
	}

	public long getPosition(){
		return position;
	}

	public void set(String word, int fileId, long position){
		setWord(word);
		setFileId(fileId);
		setPosition(position);
	}

	public void setWord(String word){
		this.word.set(word);
	}

	public void setFileId(int fileId){
		this.fileId = fileId;
	}

	public void setPosition(long position){
		this.position = position;
	}

	public int hashcode(){
		return getWord().hashCode() + getFileId();
	}
}