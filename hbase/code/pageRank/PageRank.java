package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PageRank {

	public static void main(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path tmp_inputPath = new Path(PageRankSetting.TMP_INPUT_PATH);
		Path tmp_outputPath = new Path(PageRankSetting.TMP_OUTPUT_PATH);

		long totalNodeCount = runParseInputJob(inputPath, tmp_inputPath);
		System.out.println("Total Node Count = "+totalNodeCount);
		int iterationCount = 0;

		long upscaledConvergenceError;
		while( true ){
			iterationCount += 1;
			System.out.println("==="+iterationCount+"===");
			upscaledConvergenceError = calculatePageRank(tmp_inputPath, tmp_outputPath, totalNodeCount);

			Path tmp = tmp_inputPath;
			tmp_inputPath = tmp_outputPath;
			tmp_outputPath = tmp;

			if ( isConverged(upscaledConvergenceError) ){
				break;
			}
		}
		sortResult(tmp_inputPath, outputPath);
		System.out.println("Total iterations = "+iterationCount);
	}

	private static long runParseInputJob(Path input, Path output) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ParseInput");
		job.setJarByClass(PageRank.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(ParseInputMapper.class);
		job.setPartitionerClass(ParseInputPartitioner.class);
		job.setReducerClass(ParseInputReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(PageRankSetting.PARSE_INPUT_REDUCER_COUNT);

		// add input/output path
		clearOutputDirectory(conf, output);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);

		Counter totalNodeCounter = job.getCounters().findCounter(NodeTypeCounter.TOTAL_NODE);
		long totalNodeCount = totalNodeCounter.getValue();
		return totalNodeCount;
	}

	private static long calculatePageRank(Path input, Path output, long totalNodeCount) throws Exception {
		Configuration conf = new Configuration();
		conf.setLong(PageRankSetting.TOTAL_NODE_COUNT_KEY, totalNodeCount);

		Job job = Job.getInstance(conf, "CalculatePageRank");
		job.setJarByClass(PageRank.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreNeighborNodesPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ScoreNeighborNodesPair.class);

		// set the number of reducer
		job.setNumReduceTasks(30);

		// add input/output path
		clearOutputDirectory(conf, output);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);

		long totalDeadEnd = job.getCounters().findCounter(NodeTypeCounter.TOTAL_DEAD_END).getValue();
		long totalDeadEndScore = job.getCounters().findCounter(NodeTypeCounter.TOTAL_DEAD_END_SCORE).getValue();
		long upscaledConvergenceError = job.getCounters().findCounter(NodeTypeCounter.CONVERENCE_ERROR).getValue();
		double convergenceError = 1.0*upscaledConvergenceError/PageRankSetting.UPSCALE_FACTOR;
		double totalPageRank = 1.0*job.getCounters().findCounter(NodeTypeCounter.TOTAL_PAGERANK).getValue()/PageRankSetting.UPSCALE_FACTOR;

		System.out.println("Total Dead End = "+totalDeadEnd);
		System.out.println("Total Dead End Score = "+totalDeadEndScore);
		System.out.println("Total Page Rank = "+totalPageRank);
		System.out.println("Convergence error = "+convergenceError);
		System.out.println();
		if ( totalPageRank < 0.999999 ){
			//throw new Exception("MYERROR: Total Page Rank leaking.");
			System.out.println("MYERROR: Total Page Rank leaking.");
		}

		return upscaledConvergenceError;
	}

	private static void sortResult(Path input, Path output) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SortResult");
		job.setJarByClass(PageRank.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(SortResultMapper.class);
		job.setReducerClass(SortResultReducer.class);
		job.setPartitionerClass(SortResultPartitioner.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(TitleScorePair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// set the number of reducer
		job.setNumReduceTasks(7);

		// add input/output path
		clearOutputDirectory(conf, output);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

	private static void clearOutputDirectory(Configuration conf, Path outputPath) throws Exception{
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath,true);
		}
		fileSystem.close();
	}

	private static boolean isConverged(long upscaledConvergenceError){
		if ( upscaledConvergenceError >= PageRankSetting.UPSCALE_ERROR_BOUND ){
			return false;
		}
		else{
			return true;
		}
	}
}
