package invertedIndex;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.LongWritable;

public class WordFilePartitioner extends Partitioner<WordFileIdPositionPair, LongWritable>{

	@Override
	public int getPartition(WordFileIdPositionPair key, LongWritable value, int numPartitions){
		if ( key.hashcode() >= 0){
			return  key.hashcode() % numPartitions;
		}
		else{
			return (-1*key.hashcode()) % numPartitions;
		}
	}
}