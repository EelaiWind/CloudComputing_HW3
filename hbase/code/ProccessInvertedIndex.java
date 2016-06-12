package myHbase;

import java.util.ArrayList;
import java.util.List;

public class ProccessInvertedIndex{
    static public int main(String[] args) throws Exception{
        String invertedIndexTableName = args[0];
        String invertedIndexOutput = args[1];

        HbaseUtils.buidInvertedIndexTable(invertedIndexTableName, invertedIndexOutput);
        return 0;
    }
}