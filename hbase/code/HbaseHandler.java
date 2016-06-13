package myHbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

public class HbaseHandler {

    /* API can be found here:
        https://hbase.apache.org/apidocs/
    */
    private Admin admin;
    private String tableName;
    private Table table;
    // A buffer to tempory store for put request, used to speed up the process of putting records into hbase
    private List<Put> putList;
    private static final int listCapacity = 1000000;

    public HbaseHandler(String tableName) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        putList = new ArrayList<Put>(listCapacity);
        this.tableName = tableName;
        this.table = connection.getTable(TableName.valueOf(tableName));
    }

    public void createTable(String[] colFamilies) throws Exception {
        // Instantiating table descriptor class
        TableName hTableName = TableName.valueOf(tableName);
        if (admin.tableExists(hTableName)) {
            System.out.println(tableName + " : Table already exists!");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(hTableName);
            // TODO: Adding column families to table descriptor [V]
            for (String cf : colFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            // TODO: Admin creates table by HTableDescriptor instance [V]
            System.out.println("Creating table: " + tableName + "...");
            admin.createTable(tableDescriptor);
            System.out.println("Table created");
        }
    }

    public void removeTable() throws Exception {
        TableName hTableName = TableName.valueOf(tableName);
        if (!admin.tableExists(hTableName)) {
            System.out.println(tableName + ": Table does not exist!");
        } else {
            System.out.println("Deleting table: " + tableName + "...");
            // TODO: disable & drop table [V]
            admin.disableTable(hTableName);
            admin.deleteTable(hTableName);
            System.out.println("Table deleted");
        }
    }

    public void addRecordToPutList(String rowKey, String colFamily, String[] qualifiers, String[] values) throws Exception {
        // TODO: use Put to wrap information and put it to PutList. [V]
        Put put = new Put(rowKey.getBytes());
        if (qualifiers.length != values.length){
            throw new Exception("addRecordToPutList(): qualifier list and value list size mismatch");
        }
        byte[] b_colFamily = colFamily.getBytes();
        for (int i = 0; i < values.length; i++){
            put.addColumn(b_colFamily, qualifiers[i].getBytes(), values[i].getBytes());
        }
        putList.add(put);
        if ( putList.size() == listCapacity ){
            addRecordToHBase();
        }
    }

    public void addRecordToPutList(String rowKey, String colFamily, String qualifier, String value) throws Exception {
        // TODO: use Put to wrap information and put it to PutList. [V]
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), qualifier.getBytes(), value.getBytes());
        putList.add(put);
        if ( putList.size() == listCapacity ){
            addRecordToHBase();
        }
    }

    public void addRecordToHBase() throws Exception {
        // TODO: dump things from memory (PutList) to HBaseConfiguration [V]
        table.put(putList);
        putList.clear();
    }

    public void deleteRecord(String rowKey) throws Exception {
        // TODO use Delete to wrap key information and use Table api to delete it. [V]
        Delete deletedLine = new Delete(rowKey.getBytes());
        table.delete(deletedLine);
    }

    public String getRow(String rowKey, String colFamily, String qualifier) throws Exception {
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        if (result.isEmpty()){
            throw new Exception("There is no any row key = "+rowKey);
        }
        NavigableMap<byte[],NavigableMap<byte[],byte[]>> response = result.getNoVersionMap();
        return new String(response.get(colFamily.getBytes()).get(qualifier.getBytes()));
    }

    public List<String> getPageRankAndNodeNames(List<String> rowKeys) throws Exception {
        List<Get> getList = new ArrayList<Get>();
        List<String> resultList = new ArrayList<String>();

        byte[] b_colFamily = HbaseSetting.DATA.getBytes();
        byte[] b_nodeName = HbaseSetting.NODE_NAME.getBytes();
        byte[] b_pageRank = HbaseSetting.PAGE_RANK.getBytes();

        for ( String rowKey : rowKeys ){
            getList.add(new Get(rowKey.getBytes()));
        }
        for ( Result result : table.get(getList) ){
            if (result.isEmpty()){
                throw new Exception("There is no any row key = "+result.getRow());
            }
            String nodeName = new String(result.getNoVersionMap().get(b_colFamily).get(b_nodeName));
            String pageRank = new String(result.getNoVersionMap().get(b_colFamily).get(b_pageRank));
            resultList.add(pageRank+" "+nodeName);
        }
        return resultList;
    }

    public List<String> scan(String colFamily, String qualifier) throws Exception{
        // Instantiating the Scan class
        Scan scan = new Scan();
        List<String> scanRsults = new ArrayList<String>();
        // Scanning the required columns
        byte[] b_colFamily = colFamily.getBytes();
        byte[] b_qualifier = qualifier.getBytes();
        scan.addColumn(b_colFamily, b_qualifier);

        // Getting the scan result
        ResultScanner scanner = table.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            String key = new String(result.getRow());
            String value = new String(result.getNoVersionMap().get(b_colFamily).get(b_qualifier));
            scanRsults.add(key+"\t"+value);
        }

        //closing the scanner
        scanner.close();

        return scanRsults;
    }
}