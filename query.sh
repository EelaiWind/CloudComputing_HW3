PAGERANK_TABLE_NAME=s101062105:PageRank
INVERTEDINDEX_TABLE_NAME=s101062105:InvertedIndex
DOCUMENT_TABLE_NAME=s101062105:Document

INPUT_PATH=/shared/HW2/sample-in/input-100M
PAGERANK_RESULT=/user/s101062105/hw3/output/pageRank

ID_FILE_PATH=/user/s101062105/hw3/invertedIndex/id/file_id.txt
INVERTEDINDEX_OUTPUT=/user/s101062105/hw3/invertedIndex/output

hadoop jar hbase/Hbase.jar myHbase.Query $PAGERANK_TABLE_NAME $INVERTEDINDEX_TABLE_NAME $DOCUMENT_TABLE_NAME $*