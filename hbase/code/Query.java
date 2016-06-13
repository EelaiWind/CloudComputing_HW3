package myHbase;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;

public class Query {
    static public int main(String[] args) throws Exception{
        String pageRankTableName = args[0];
        String invertedIndexTableName = args[1];
        String documentTableName = args[2];
        int totalDocumentsCount;

        FileReader reader = new FileReader(HbaseSetting.DOCUMENT_COUNT_FILE_NAME);
        BufferedReader bufferedReader = new BufferedReader(reader);
        totalDocumentsCount = Integer.valueOf(bufferedReader.readLine());
        bufferedReader.close();
        reader.close();

        List<String> queryWords = new ArrayList<String>();
        if (args.length > 3){
            System.out.print("Query = ");
            for ( int i = 3; i < args.length; i++ ){
                queryWords.add(args[i]);
                System.out.print(args[i]+",");
            }

            System.out.println(HbaseUtils.getTop10Result(pageRankTableName, invertedIndexTableName, documentTableName, totalDocumentsCount, queryWords));
        }
        return 0;
    }
}