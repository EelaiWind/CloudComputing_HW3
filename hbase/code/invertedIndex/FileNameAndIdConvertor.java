package invertedIndex;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class FileNameAndIdConvertor{
	private HashMap<String, Integer> fileNameToId;
	private HashMap<Integer, String> idToFileName;
	private int totalFileCount;

	public FileNameAndIdConvertor(JobContext context) throws IOException {
		fileNameToId = new HashMap<String, Integer>();
		idToFileName = new HashMap<Integer, String>();
		loadFileId(context);
	}

	private void loadFileId(JobContext context) throws IOException{
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		URI[] cacheFiles = context.getCacheFiles();
		Path idFilePath = new Path(cacheFiles[0].getPath());
		BufferedReader reader = new BufferedReader(
			new InputStreamReader(fileSystem.open(idFilePath))
		);

		String line;
		totalFileCount = 0;
		while((line = reader.readLine() ) != null){
			String[] parameter = line.split("\t", 2);
			if (parameter.length < 2 ){
				continue;
			}
			int id = Integer.parseInt(parameter[0]);
			String fileName = parameter[1];
			fileNameToId.put( fileName, id );
			idToFileName.put( id, fileName );
			totalFileCount += 1;
		}

		reader.close();
	}

	public int getFileId(String fileName) throws IOException{
		if ( !fileNameToId.containsKey(fileName) ){
			throw new IOException("There is no id mapping for file \""+fileName+"\"");
		}
		else{
			return fileNameToId.get(fileName);
		}
	}

	public String getFileName(int fileId) throws IOException{
		if ( !idToFileName.containsKey(fileId)){
			throw new IOException("There is no file name mapping for id \""+fileId+"\"");
		}
		else{
			return idToFileName.get(fileId);
		}
	}

	public int getTotalFileCount(){
		return totalFileCount;
	}
}