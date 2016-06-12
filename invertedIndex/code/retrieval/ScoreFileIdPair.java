package invertedIndex;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.Math;

public class ScoreFileIdPair implements WritableComparable{
	private double score;
	private int fileId;
	public ScoreFileIdPair(){
		score = 0;
		fileId = 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(score);
		out.writeInt(fileId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		score = in.readDouble();
		fileId = in.readInt();
	}

	@Override
	public int compareTo(Object another) {
		ScoreFileIdPair obj = (ScoreFileIdPair) another;

		int scoreComparision = Double.compare(getScore(), obj.getScore());
		if ( scoreComparision == 0 ){
			return Integer.compare( getFileId(), obj.getFileId() );
		}
		else{
			return -1*scoreComparision;
		}
	}

	public double getScore(){
		return score;
	}

	public int getFileId(){
		return fileId;
	}

	public void setScore(double score){
		this.score = score;
	}

	public void setFileId(int fileId){
		this.fileId = fileId;
	}

}