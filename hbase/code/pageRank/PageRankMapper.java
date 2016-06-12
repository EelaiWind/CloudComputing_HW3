package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PageRankMapper extends Mapper<LongWritable, Text, Text, ScoreNeighborNodesPair> {
	private Text outputKey = new Text();
	private ScoreNeighborNodesPair outputValue = new ScoreNeighborNodesPair();
	private double localDeadEndScoreSum;
	private long localDeadNodeCount;
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		localDeadEndScoreSum = 0;
		localDeadNodeCount= 0;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] token = value.toString().split("\\t",3);

		if ( token.length == 2 ){
			double sourceScore = Double.parseDouble(token[1]);
			localDeadEndScoreSum += sourceScore;
			passLinkAndPreviousScore(context, token[0], "", sourceScore);
			localDeadNodeCount += 1;
		}else if ( token.length == 3 ){
			double sourceScore = Double.parseDouble(token[1]);
			if ( token[2].length() == 0){
				throw new IOException("ERROR: empty links after tab");
			}
			String[] neighborNodes = token[2].split("\\t");
			int outDegree = neighborNodes.length;
			double score  = sourceScore/outDegree;

			for (String node : neighborNodes){
				distributePageRank(context, node, score);
			}
			passLinkAndPreviousScore(context, token[0], token[2], sourceScore);
		}
		else{
			throw new IOException("MYERROR: There are unexpected tab(\\t) in links");
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		collectDeadEndScore(context, localDeadEndScoreSum);
		context.getCounter(NodeTypeCounter.TOTAL_DEAD_END).increment(localDeadNodeCount);
	}

	private void collectDeadEndScore(Context context, double deadEndScore){
		System.out.println("MYLOG: deadEndScore = "+deadEndScore);
		long upcaledScore = (long) (deadEndScore*PageRankSetting.UPSCALE_FACTOR);
		context.getCounter(NodeTypeCounter.TOTAL_DEAD_END_SCORE).increment(upcaledScore);
	}

	private void passLinkAndPreviousScore(Context context, String source, String neighborNodes, double score)throws IOException, InterruptedException{
		outputKey.set(source);
		outputValue.setIsDataPair(true);
		outputValue.setScore(score);
		outputValue.setNeighborNiodes(neighborNodes);
		context.write(outputKey,outputValue);
	}

	private void distributePageRank(Context context, String neighborNode, double score) throws IOException, InterruptedException{
		outputKey.set(neighborNode);
		outputValue.setIsDataPair(false);
		outputValue.setScore(score);
		outputValue.setNeighborNiodes("");
		context.write(outputKey,outputValue);
	}
}
