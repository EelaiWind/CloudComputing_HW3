PAGERANK_TABLE_NAME=s101062105:PageRank
INVERTEDINDEX_TABLE_NAME=s101062105:InvertedIndex
DOCUMENT_TABLE_NAME=s101062105:Document

INPUT_PATH=/shared/HW2/sample-in/input-100M
PAGERANK_OUTPUT=/user/s101062105/hw3/output/pageRank

ID_FILE_PATH=/user/s101062105/hw3/invertedIndex/id/file_id.txt
INVERTEDINDEX_OUTPUT=/user/s101062105/hw3/output/invertedIndex/

### run PageRank
spark-submit --class PageRank --num-executors 30 pageRank/target/scala-2.10/pagerank-application_2.10-1.0.jar ${INPUT_PATH} $PAGERANK_OUTPUT
hadoop fs -getmerge ${PAGERANK_OUTPUT} pageRank.log

hadoop jar hbase/Hbase.jar myHbase.ProccessPageRank $PAGERANK_TABLE_NAME $DOCUMENT_TABLE_NAME $INPUT_PATH $PAGERANK_OUTPUT $ID_FILE_PATH

### run InvertedIndex
hdfs dfs -rm -r $INVERTEDINDEX_OUTPUT
hadoop jar invertedIndex/InvertedIndex.jar invertedIndex.InvertedIndexJob $INPUT_PATH $INVERTEDINDEX_OUTPUT
hadoop fs -cat ${INVERTEDINDEX_OUTPUT}/* > invertedIndextable.log

hadoop jar hbase/Hbase.jar myHbase.ProccessInvertedIndex $INVERTEDINDEX_TABLE_NAME $INVERTEDINDEX_OUTPUT
