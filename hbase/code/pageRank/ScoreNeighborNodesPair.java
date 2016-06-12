package pageRank;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashSet;
import java.util.Collection;

public class ScoreNeighborNodesPair implements Writable{
	private double score;
	private Text neighborNodes;
	private boolean isDataPair;

	public ScoreNeighborNodesPair(){
		score = 0;
		neighborNodes = new Text();
		isDataPair = false;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isDataPair);
		out.writeDouble(score);
		neighborNodes.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		isDataPair = in.readBoolean();
		score = in.readDouble();
		neighborNodes.readFields(in);
	}

	public void setIsDataPair(boolean isDataPair){
		this.isDataPair = isDataPair;
	}

	public void setScore(double score){
		this.score = score;
	}

	public void setNeighborNiodes(String neighborNodes){
		this.neighborNodes.set(neighborNodes);
	}

	public boolean isDataPair(){
		return this.isDataPair;
	}

	public double getScore(){
		return this.score;
	}

	public String getNeighborNodes(){
		return this.neighborNodes.toString();
	}

	public String toString(){
		if ( "".equals(neighborNodes.toString()) ){
			return Double.toString(score);
		}
		else{
			return Double.toString(score)+"\t"+neighborNodes;
		}
	}

}