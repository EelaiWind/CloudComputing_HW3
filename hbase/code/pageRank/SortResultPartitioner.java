package pageRank;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.NullWritable;

public class SortResultPartitioner extends Partitioner<TitleScorePair, NullWritable>{

	@Override
	public int getPartition(TitleScorePair key, NullWritable value, int numPartitions){
		char firstLetter = key.getTitle().charAt(0);
		if (firstLetter < 'A'){
			return Math.max(0,numPartitions-1);
		}
		else if ( firstLetter < 'E' ){
			return Math.max(1,numPartitions-1);
		}
		else if ( firstLetter < 'I' ){
			return Math.max(2,numPartitions-1);
		}
		else if ( firstLetter < 'O' ){
			return Math.max(3,numPartitions-1);
		}
		else if ( firstLetter < 'U' ){
			return Math.max(4,numPartitions-1);
		}
		else if ( firstLetter < 'Z' ){
			return Math.max(5,numPartitions-1);
		}
		else{
			return Math.max(6,numPartitions-1);
		}
	}
}