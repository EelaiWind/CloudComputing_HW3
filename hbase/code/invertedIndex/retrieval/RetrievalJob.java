package invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;
import org.apache.hadoop.fs.Path;

public class RetrievalJob {

	public static int main(String[] args) throws Exception {
		if ( args.length < 4 ){
			System.out.println("Usage: hadoop jar InvertedIndex.jar invertedIndex.RetrievalJob <inverted indextable path> <retrieval output path> <input documents path> [-i] <query string1, query string2 ...> ");
			return 1;
		}
		Configuration conf = new Configuration();
		String queries = "";
		String ignoreCase ="";
		for ( int i = 3 ; i < args.length; i++){
			if ( i == 3 && args[i].equals("-i")){
				conf.setBoolean(InvertedIndexSetting.IGNORE_LETTER_CASE,true);
				ignoreCase = "\t(Ignore Case different)";
			}
			else{
				queries += " "+args[i];
			}
		}
		conf.set(InvertedIndexSetting.INPUT_FILES_DIR,args[2]);
		conf.set(InvertedIndexSetting.CONF_QUERY_KEY,queries);
		System.out.println("Search for word(s) :"+queries+ignoreCase);

		Job job = Job.getInstance(conf, "RetrievalJob");
		job.setJarByClass(RetrievalJob.class);

		job.addCacheFile(new URI(InvertedIndexSetting.ID_FILE_PATH));

		// set the class of each stage in mapreduce
		job.setMapperClass(RetrievalMapper.class);
		job.setReducerClass(RetrievalReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(ScoreFileIdPair.class);
		job.setMapOutputValueClass(QueryAnswer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(1);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}