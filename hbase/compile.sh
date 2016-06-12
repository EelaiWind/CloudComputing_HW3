rm -r class/*
javac -Xlint -classpath $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:$HADOOP_HOME/share/hadoop/common/lib/hadoop-annotations-2.7.2.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar:$HBASE_HOME/lib/hbase-common-1.2.1.jar:$HBASE_HOME/lib/hbase-client-1.2.1.jar -d class code/*.java
jar -cvf Hbase.jar -C class/ .
