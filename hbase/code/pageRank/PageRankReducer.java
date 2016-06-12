package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends Reducer<Text,ScoreNeighborNodesPair,Text,ScoreNeighborNodesPair> {
	private ScoreNeighborNodesPair outputValue = new ScoreNeighborNodesPair();
	private long totalNodeCount;
	private double alpha;
	private double totalDeadEndScoreSum;
	private double localConvergenceErrorSum;
	private double localTotalPageRank;
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		totalNodeCount = context.getConfiguration().getLong(PageRankSetting.TOTAL_NODE_COUNT_KEY,-1);
		if (totalNodeCount == -1){
			throw new IOException("MYERROR: "+PageRankSetting.TOTAL_NODE_COUNT_KEY+" is not set in configuration");
		}
		totalDeadEndScoreSum = 1.0*PageRankUtils.getCounter(context, NodeTypeCounter.TOTAL_DEAD_END_SCORE).getValue()/PageRankSetting.UPSCALE_FACTOR;
		alpha = PageRankSetting.ALPHA;
		localConvergenceErrorSum = 0;
		localTotalPageRank = 0;
	}

	@Override
	public void reduce(Text key, Iterable<ScoreNeighborNodesPair> values, Context context) throws IOException, InterruptedException {
		double newPageRank = 0;
		double previousPageRank = 0;
		for ( ScoreNeighborNodesPair value : values ){
			if ( value.isDataPair() ){
				previousPageRank = value.getScore();
				outputValue.setNeighborNiodes(value.getNeighborNodes());
			}
			else{
				newPageRank += value.getScore();
			}
		}

		newPageRank = alpha*newPageRank + ((1-alpha)*1 + alpha * totalDeadEndScoreSum)/totalNodeCount;

		localConvergenceErrorSum += Math.abs(previousPageRank - newPageRank);
		localTotalPageRank += newPageRank;
		outputValue.setScore(newPageRank);
		context.write(key, outputValue);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		addUpConvergenceError(context, localConvergenceErrorSum);
		addUpTotalPageRank(context, localTotalPageRank);
	}

	private void addUpConvergenceError(Context context, double error){
		long upcaledError = (long) (error*PageRankSetting.UPSCALE_FACTOR);
		context.getCounter(NodeTypeCounter.CONVERENCE_ERROR).increment(upcaledError);
	}

	private void addUpTotalPageRank(Context context, double pageRank){
		long upcaledPageRank = (long) (pageRank*PageRankSetting.UPSCALE_FACTOR);
		context.getCounter(NodeTypeCounter.TOTAL_PAGERANK).increment(upcaledPageRank);
	}
}
