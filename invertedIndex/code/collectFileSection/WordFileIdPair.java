package invertedIndex;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class WordFileIdPair implements WritableComparable{
	private Text word = new Text();
	private int fileId;

	public WordFileIdPair(){

	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		out.writeInt(fileId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		fileId = in.readInt();
	}

	@Override
	public int compareTo(Object another) {
		WordFileIdPair obj = (WordFileIdPair) another;

		int wordComparision = getWord().compareTo(obj.getWord());
		if ( wordComparision == 0 ){
			return Integer.compare(getFileId(), obj.getFileId());
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


	public void set(String word, int fileId){
		setWord(word);
		setFileId(fileId);
	}

	public void setWord(String word){
		this.word.set(word);
	}

	public void setFileId(int fileId){
		this.fileId = fileId;
	}

	public int hashcode(){
		return getWord().hashCode() + getFileId();
	}
}