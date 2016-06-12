package invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;

public class FileIdRecorder{
	private Configuration conf;

	public FileIdRecorder(Configuration conf){
		this.conf = conf;
	}
	public void writeFileIDs(String filesDirectory) throws IOException{
		writeFileIDs(new Path(filesDirectory));
	}

	public void writeFileIDs(Path filesDirectory) throws IOException{
		FileSystem fileSystem = FileSystem.get(conf);

		BufferedWriter writer = new BufferedWriter(
			new OutputStreamWriter(
				fileSystem.create( new Path(InvertedIndexSetting.ID_FILE_PATH), true ) ,"UTF-8" )
		);
		int id = 1;
		for (FileStatus status : fileSystem.listStatus(filesDirectory)) {
			writer.write(id+" "+ status.getPath().getName()+"\n");
			id += 1;
		}
		writer.close();
		fileSystem.close();
	}
}