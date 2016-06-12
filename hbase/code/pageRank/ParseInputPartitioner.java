package pageRank;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class ParseInputPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numPartitions){
		String keyString = key.toString();
		if ( PageRankUtils.isSpecialKey(keyString) ){
			return Integer.parseInt(keyString.substring(1,keyString.length()-1));
		}
		else{
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}
}