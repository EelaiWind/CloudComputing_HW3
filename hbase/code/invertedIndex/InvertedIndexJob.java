package invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class InvertedIndexJob{
	private static Path inputPath;
	private static Path outputPath;
	private static final Path tmp_output_path = new Path(InvertedIndexSetting.TMP_INVERTED_INDEX_OUTPUT_DIR);

	public static int main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		collectPosition(conf);
		collectFileSection(conf);
		return 0;
	}

	private static int collectPosition(Configuration conf)throws Exception{
		Job job = Job.getInstance(conf, "FileSectionJob");
		job.setJarByClass(InvertedIndexJob.class);

		job.addCacheFile(new URI(InvertedIndexSetting.ID_FILE_PATH));

		// set the class of each stage in mapreduce
		job.setMapperClass(FileSectionMapper.class);
		job.setReducerClass(FileSectionReducer.class);

		job.setGroupingComparatorClass(FileSectionGroupComparator.class);
		job.setPartitionerClass(WordFilePartitioner.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(WordFileIdPositionPair.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(10);

		// add input/output path
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(tmp_output_path)){
			fileSystem.delete(tmp_output_path,true);
		}
		fileSystem.close();
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, tmp_output_path);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static int collectFileSection(Configuration conf)throws Exception{
		Job job = Job.getInstance(conf, "CollectFileSectionJob");
		job.setJarByClass(InvertedIndexJob.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(CollectFileSectionMapper.class);
		job.setReducerClass(CollectFileSectionReducer.class);

		job.setGroupingComparatorClass(WordFileIdGroupComparator.class);
		//job.setPartitionerClass(WordFilePartitioner.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(WordFileIdPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(1);

		// add input/output path
		FileInputFormat.addInputPath(job, tmp_output_path);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}