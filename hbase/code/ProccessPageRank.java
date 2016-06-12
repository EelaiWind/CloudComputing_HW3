package myHbase;

import java.util.ArrayList;
import java.util.List;

public class ProccessPageRank {
    static public int main(String[] args) throws Exception{
        String pageRankTableName = args[0];
        String documentTableName = args[1];
        String pageRankInputPath = args[2];
        String pageRankOutputPath = args[3];
        String idFileOutputPath = args[4];

        HbaseUtils.buildPageRankTable(pageRankTableName, pageRankInputPath ,pageRankOutputPath, idFileOutputPath);
        HbaseUtils.buildDocumentTable(documentTableName, pageRankInputPath);
        return 0;
    }
}