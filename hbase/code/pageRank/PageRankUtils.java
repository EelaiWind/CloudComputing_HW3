package pageRank;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;

public class PageRankUtils{

	public static Counter getCounter(Context context, NodeTypeCounter type) throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		return currentJob.getCounters().findCounter(type);
	}

	public static boolean isSpecialKey(String key){
		int keyLength = key.length();
		if ( key.matches("^ \\d+<$") ){
			return true;
		}
		else{
			return false;
		}
	}
}