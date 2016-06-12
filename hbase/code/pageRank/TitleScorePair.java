package pageRank;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.Math;

public class TitleScorePair implements WritableComparable{
	private Text title;
	private double score;

	public TitleScorePair(){
		title = new Text();
		score = 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		title.write(out);
		out.writeDouble(score);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title.readFields(in);
		score = in.readDouble();
	}

	@Override
	public int compareTo(Object another) {
		TitleScorePair obj = (TitleScorePair) another;

		int scoreComparision = Double.compare(getScore(), obj.getScore());
		if ( scoreComparision == 0 ){
			return this.getTitle().compareTo(obj.getTitle());
		}
		else{
			return -1*scoreComparision;
		}
	}

	public double getScore(){
		return score;
	}

	public String getTitle(){
		return this.title.toString();
	}

	public void setScore(double score){
		this.score = score;
	}

	public void setTitle(String title){
		this.title.set(title);
	}

}