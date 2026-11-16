import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseAPI {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 1. 创建表
    public static void createTable(String tableName, String[] fields) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
            System.out.println("表 " + tableName + " 已存在，已删除旧表。");
        }
        
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (String field : fields) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(field));
        }
        admin.createTable(builder.build());
        System.out.println("表 " + tableName + " 创建成功！");
    }

    // 2. 添加记录
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] parts = fields[i].split(":");
            put.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]), Bytes.toBytes(values[i]));
        }
        table.put(put);
        table.close();
        System.out.println("向表 " + tableName + " 行 " + row + " 添加数据成功！");
    }

    // 3. 浏览数据
    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (column.contains(":")) {
            String[] parts = column.split(":");
            scan.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]));
        } else {
            scan.addFamily(Bytes.toBytes(column));
        }
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("--- 开始扫描表 " + tableName + " 的列 " + column + " ---");
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(cell)) + 
                                   " , Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        scanner.close();
        table.close();
    }

    // 4. 修改数据
    public static void modifyData(String tableName, String row, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        String[] parts = column.split(":");
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]), Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("表 " + tableName + " 行 " + row + " 列 " + column + " 修改成功！");
    }

    // 5. 删除行
    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
        table.close();
        System.out.println("表 " + tableName + " 行 " + row + " 删除成功！");
    }

    public static void main(String[] args) {
        try {
            String tableName = "student_test";
            createTable(tableName, new String[]{"info", "score"});
            addRecord(tableName, "2015001", 
                new String[]{"info:name", "info:sex", "score:Math"}, 
                new String[]{"Zhangsan", "male", "86"});
            scanColumn(tableName, "info");
            scanColumn(tableName, "score:Math");
            modifyData(tableName, "2015001", "score:Math", "95");
            scanColumn(tableName, "score:Math");
            deleteRow(tableName, "2015001");
            scanColumn(tableName, "info");
            System.out.println("所有 API 测试执行完毕！");
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}