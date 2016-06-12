package myHbase;

import java.util.ArrayList;
import java.util.List;

public class Query {
    static public int main(String[] args) throws Exception{
        String pageRankTableName = args[0];
        String invertedIndexTableName = args[1];
        String documentTableName = args[2];

        List<String> queryWords = new ArrayList<String>();
        if (args.length > 3){
            System.out.print("Query = ");
            for ( int i = 3; i < args.length; i++ ){
                queryWords.add(args[i]);
                System.out.print(args[i]+",");
            }

            System.out.println(HbaseUtils.getTop10Result(pageRankTableName, invertedIndexTableName, documentTableName, 30727, queryWords));
        }
        return 0;
    }
}