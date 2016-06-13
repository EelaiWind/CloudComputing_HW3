package myHbase;

import java.io.*;
import java.util.*;
import java.net.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

public class HbaseUtils {

    static class ScoreData {
        public double pageRank = 0;
        public double tfidf = 0;
        public double totalScore = 0;
    }

    public static void buildPageRankTable(String pageRankTableName, String pageRankInputPath ,String pageRankOutputPath, String idFileOutputPath) throws Exception{
        HbaseHandler hbseHadler = new HbaseHandler(pageRankTableName);
        HashMap<String, String> documentToId = new HashMap<String, String>();

        hbseHadler.removeTable();
        hbseHadler.createTable(HbaseSetting.colFamily);
        int file_id = 0;
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(pageRankOutputPath));
        String[] valueList = new String[2];
        int count = 0;
        String line;

        for (int i=0;i<status.length;i++){
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            count += 1;
            line=br.readLine();
            while (line != null){
                String[] tokens = line.split("\t");
                if (tokens.length > 2){
                    throw new  Exception("buildPageRankTable() : There is more than one tab in one line!");
                }
                valueList[0] = tokens[0];
                valueList[1] = tokens[1];
                documentToId.put(valueList[0], String.valueOf(file_id));
                hbseHadler.addRecordToPutList(String.valueOf(file_id++), HbaseSetting.DATA, HbaseSetting.pageRankQualifier, valueList);
                line=br.readLine();
            }
            br.close();
        }
        // upload remaining Puts in buffer to Hbase
        hbseHadler.addRecordToHBase();
        fs.close();
        System.out.println("MYLOG : write "+count+" pageRank to Hbase");
        Writer os = new FileWriter(HbaseSetting.DOCUMENT_COUNT_FILE_NAME);
        os.write(String.valueOf(file_id));
        os.close();

        generateIdMappingFile(documentToId, idFileOutputPath);
    }

    private static void generateIdMappingFile(HashMap<String,String> documentToId, String idFileOutputPath) throws Exception{
        FileSystem fileSystem = FileSystem.get(new Configuration());

        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(
                fileSystem.create( new Path(idFileOutputPath), true ) ,"UTF-8" )
        );
        int count = 0;
        for (Map.Entry<String, String> entry : documentToId.entrySet() ) {
            writer.write(entry.getValue()+"\t"+entry.getKey()+"\n");
            count += 1;
        }
        writer.close();
        fileSystem.close();
        System.out.println("MYLOG : ID mapping file containing "+count+" nodes");
    }

    public static void buidInvertedIndexTable(String invertedIndexTableName, String invertedIndexOutput) throws Exception{
        final HbaseHandler invertedIndexTabeHadler = new HbaseHandler(invertedIndexTableName);


        invertedIndexTabeHadler.removeTable();
        invertedIndexTabeHadler.createTable(HbaseSetting.colFamily);

        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(invertedIndexOutput));
        int lineCount = 0;
        for (int i=0;i<status.length;i++){
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            String line=br.readLine();
            while (line != null){
                lineCount += 1;
                String[] tokens = line.split("\t",2);
                invertedIndexTabeHadler.addRecordToPutList(tokens[0], HbaseSetting.DATA, HbaseSetting.DATA, tokens[1]);
                line=br.readLine();
            }
            br.close();
        }
        // upload remaining Puts in buffer to Hbase
        invertedIndexTabeHadler.addRecordToHBase();
        fs.close();
        System.out.println("MYLOG : write "+lineCount+" word to InvertedIndex table");
    }

    public static String getTopTenResult(String pageRankTableName, String invertedIndexTableName, String documentTableName, int totalDocumentCount, List<String> queryWords) throws Exception{
        final int topTen = 10;
        HbaseHandler pageRankTableHandler = new HbaseHandler(pageRankTableName);
        HbaseHandler invertedIndexHandler = new HbaseHandler(invertedIndexTableName);
        HbaseHandler documentTableHandler = new HbaseHandler(documentTableName);
        HashMap<String, Integer> documentIdAndFrequency = getIntersectionDocuments(invertedIndexHandler, queryWords);
        List<Map.Entry<String, ScoreData>> documentNameToTotalScore = getTotalScore(pageRankTableHandler, totalDocumentCount, documentIdAndFrequency);
        int rank = 0;
        StringBuilder resultBuffer = new StringBuilder();
        for (Map.Entry<String, ScoreData> entry : documentNameToTotalScore){
            rank += 1;
            ScoreData data = entry.getValue();
            String document = documentTableHandler.getRow(entry.getKey(), HbaseSetting.DATA, HbaseSetting.DATA);
            resultBuffer.append("#"+rank+" ["+entry.getKey()+"]\tScore = "+data.totalScore+"\n");
            resultBuffer.append("PageRank = "+data.pageRank+", TF-IDF = "+data.tfidf+"\n");
            resultBuffer.append( getMathingegments(document, queryWords) );
            resultBuffer.append("======\n\n");
            if (rank >= topTen){
                break;
            }
        }
        return resultBuffer.toString();
    }

    private static List<Map.Entry<String, ScoreData>> getTotalScore(HbaseHandler pageRankTableHandler, int totalDocumentCount, Map<String, Integer> documentIdAndFrequency) throws Exception{
        HashMap<String, ScoreData> documentToScore = new HashMap<String, ScoreData>();
        int df = documentIdAndFrequency.size();
        List<String> documentIds = new ArrayList<String>();
        documentIds.addAll(documentIdAndFrequency.keySet());
        List<String> pageRankAndNodeNames = pageRankTableHandler.getPageRankAndNodeNames(documentIds);

        for (int i = 0 ; i < documentIds.size(); i++){
            ScoreData scoreData = new ScoreData();
            int tf = documentIdAndFrequency.get(documentIds.get(i));
            String[] tokens = pageRankAndNodeNames.get(i).split(" ",2);
            double pageRank = Double.valueOf(tokens[0]);
            double tfidf = 1.0*tf * Math.log10(1.0*totalDocumentCount/df);
            scoreData.pageRank = pageRank;
            scoreData.tfidf = tfidf;
            scoreData.totalScore = pageRank*tfidf*tfidf;
            documentToScore.put( tokens[1] , scoreData);
        }

        List<Map.Entry<String, ScoreData>> sortedList = new LinkedList<Map.Entry<String, ScoreData>>( documentToScore.entrySet() );
        Collections.sort(sortedList, new Comparator<Map.Entry<String, ScoreData>>(){
            public int compare(Map.Entry<String, ScoreData> entry1, Map.Entry<String, ScoreData> entry2){
                double score1 = entry1.getValue().totalScore;
                double score2 = entry2.getValue().totalScore;
                if (score1 > score2){
                    return -1;
                }
                else if (score1 < score2){
                    return 1;
                }
                else{
                    return entry1.getKey().compareTo(entry2.getKey());
                }
            }
        });
        return sortedList;
    }

    private static HashMap<String, Integer> getIntersectionDocuments(HbaseHandler invertedIndexHandler, List<String> queryWords)throws Exception{
        boolean isInitialized = false;
        HashMap<String, Integer> documentIdAndFrequency = new HashMap<String, Integer>();
        List<String> removedKeys = new ArrayList<String>();
        for (String queryWord : queryWords){
            if (!isInitialized){
                isInitialized = true;
                documentIdAndFrequency = getContainingDocumentsId(invertedIndexHandler, queryWord);
            }
            else{
                HashMap<String, Integer> tmp_map = getContainingDocumentsId(invertedIndexHandler, queryWord);
                removedKeys.clear();
                for (Map.Entry<String, Integer> entry : documentIdAndFrequency.entrySet()){
                    if ( tmp_map.containsKey(entry.getKey()) ){
                        // maintain "min" tf for each document
                        if ( tmp_map.get(entry.getKey()) < entry.getValue() ){
                            documentIdAndFrequency.put(entry.getKey(), tmp_map.get(entry.getKey()));
                        }
                    }
                    else{
                        removedKeys.add(entry.getKey());
                    }
                }

                for (String removedKey : removedKeys ){
                    documentIdAndFrequency.remove(removedKey);
                }
            }
        }
        return documentIdAndFrequency;
    }

    private static HashMap<String, Integer> getContainingDocumentsId(HbaseHandler invertedIndexHandler, String word)throws Exception{
        String result = invertedIndexHandler.getRow(word, HbaseSetting.DATA, HbaseSetting.DATA);
        HashMap<String, Integer> documentIdAndFrequency = new HashMap<String, Integer>();
        for (String token : result.split(";") ){
            String[] tmp_tokens = token.split(" ");
            documentIdAndFrequency.put(tmp_tokens[0], Integer.valueOf(tmp_tokens[1]));
        }
        return documentIdAndFrequency;
    }

    public static void buildDocumentTable(String tableName, String documentPath) throws Exception{
        HbaseHandler documentTableHandler = new HbaseHandler(tableName);
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(documentPath));

        documentTableHandler.removeTable();
        documentTableHandler.createTable(HbaseSetting.colFamily);
        int count = 0;
        String line;
        for (int i=0;i<status.length;i++){
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            line=br.readLine();
            while (line != null){
                count += 1;
                String title = retrieveTitle(line);
                String text = retrieveText(line);
                documentTableHandler.addRecordToPutList(title, HbaseSetting.DATA, HbaseSetting.DATA, text);
                line = br.readLine();
            }
        }
        // flush put list buffer
        documentTableHandler.addRecordToHBase();
        System.out.println("MYLOG : wrire "+count+" documents to Hbase");
    }

    private static String retrieveTitle(String document) throws Exception{
        final Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
        Matcher titleMatcher = titlePattern.matcher(document);
        if (titleMatcher.find()){
            String title = titleMatcher.group(1);
            title = replaceSpecialString(title);
            return capitalizeFirstLetter(title);
        }
        else{
            throw new Exception("retrieveText() : <title>...</title> doesn't exist");
        }
    }

    private static String retrieveText(String document) throws Exception{
        final Pattern textPattern = Pattern.compile("<text.*?>([\\S\\s]+?)</text>");
        final Pattern noTextPattern = Pattern.compile("<text.*?/>");
        Matcher textMatcher = textPattern.matcher(document);
        if (textMatcher.find()){
            String text = textMatcher.group(1);
            return replaceSpecialString(text);
        }
        else {
            if ( noTextPattern.matcher(document).find() ){
                return "";
            }
            else{
                throw new Exception("retrieveText() : <text...</text> doesn't exist");
            }
        }
    }

    private static String replaceSpecialString(String input){
        return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "'");
    }

    private static String capitalizeFirstLetter(String input){
        char firstChar = input.charAt(0);
        if ( (firstChar >= 'a' && firstChar <='z') || (firstChar>= 'A' && firstChar <= 'Z') ){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else{
                return input.substring(0, 1).toUpperCase() + input.substring(1);
            }
        }
        else{
            return input;
        }
    }

    private static String getMathingegments(String document, List<String> queryWords){
        final int segmentLength = 50;
        final int maxSegmentCount = 3;
        final int totalWordCount = queryWords.size();
        final int maxLength = document.length();
        List<String> resultBuffer = new ArrayList<String>();
        String proccesses_document;
        StringBuilder stringBuilder = new StringBuilder();
        int uniqueResultCount = 0;

        document = " "+document+" ";
        proccesses_document = document.replaceAll("[^A-Za-z]", " ");

        for (String word : queryWords){
            Matcher matcher = Pattern.compile(" "+word+" ").matcher(proccesses_document);
            int count = 0 ;
            boolean isFirst = true;
            while ( matcher.find() && count < maxSegmentCount){
                int endIndex =  Math.min(maxLength, matcher.end()+ segmentLength);
                if (isFirst){
                    isFirst = false;
                    uniqueResultCount += 1;
                    resultBuffer.add(0,"\t"+document.substring(matcher.start(), endIndex)+"\n");
                }
                else{
                    resultBuffer.add("\t"+document.substring(matcher.start(), endIndex)+"\n");
                }
                count += 1;
                if(uniqueResultCount >= maxSegmentCount){
                    break;
                }
            }
            if(uniqueResultCount >= maxSegmentCount){
                break;
            }
        }

        for (int i = 0 ; i < maxSegmentCount && i < resultBuffer.size(); i++){
            stringBuilder.append( resultBuffer.get(i) );
        }
        return stringBuilder.toString();
    }
}
