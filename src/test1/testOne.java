package test1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
 
public class testOne{
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args)throws IOException{
    	//init();
    	//close();
    	//createTable("ls_world",new String[]{"w1","w2"});
    	//insertRow("ls_world", "95001", "w2", "age", "22");
    	//insertRow("ls_world", "95001", "w2", "friend", "eeeea");
    	//deleteTable("Score");
    	//deleteRow("ls_world", "95001", "w1", "");
    	//getData("ls_world", "95001", "w1", "age");
    	listTables();
    }
    //建立连接
    public static void init(){
    	// 根据 hbase-site.xml 文件初始化 Configuration 对象
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://master:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "master,ceph1,ceph2,ceph3");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try{
        	// 根据 Configuration 对象初始化 Connection 对象
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            System.out.println("连接HBase成功.");
        }catch (IOException e){
            System.err.println("连接HBase失败.");
        }
    }
    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    /**
     * 建表。HBase的表中会有一个系统默认的属性作为主键，主键无需自行创建，默认为put命令操作中表名后第一个数据，因此此处无需创建id列
     * @param myTableName 表名
     * @param colFamily 列族名
     * @throws IOException
     */
    public static void createTable(String myTableName,String[] colFamily) throws IOException {
 
        init();
        TableName tableName = TableName.valueOf(myTableName);
 
        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
        	TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for(String str:colFamily){
            	ColumnFamilyDescriptor columnfamily = ColumnFamilyDescriptorBuilder.of(str);
            	tableDescriptor.setColumnFamily(columnfamily);
            }
            admin.createTable(tableDescriptor.build());
            System.out.println("create table success");
        }
        close();
    }
    /**
     * 删除指定表
     * @param tableName 表名
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }
    /**
     * 查看已有表
     * @throws IOException
     */
    public static void listTables() throws IOException {
        init();
        TableName tablenames[] = admin.listTableNames();
        for(TableName tablename:tablenames){
        	System.out.println(tablename.getNameAsString());
        }
        close();
    }
    /**
     * 向某一行的某一列插入数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param colFamily 列族名
     * @param col 列名（如果其列族下没有子列，此参数可为空）
     * @param val 值
     * @throws IOException
     */
    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
        close();
    }
    /**
     * 删除数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param colFamily 列族名
     * @param col 列名
     * @throws IOException
     */
    public static void deleteRow(String tableName,String rowKey,String colFamily,String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());        
        if(col.equals("")) //删除指定列族的所有数据
        	delete.addFamily(colFamily.getBytes());
        else               //删除指定列的数据
            delete.addColumn(colFamily.getBytes(), col.getBytes());
 
        table.delete(delete);
        table.close();
        close();
    }
    /**
     * 根据行键rowkey查找数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param colFamily 列族名
     * @param col 列名
     * @throws IOException
     */
    public static void getData(String tableName,String rowKey,String colFamily,String col)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        if(col.equals(""))
        	get.addFamily(colFamily.getBytes());
        else 
        	get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }
    /**
     * 格式化输出
     * @param result
     */
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
        	System.out.println(new String(CellUtil.cloneFamily(cell))+":"
                              +new String(CellUtil.cloneQualifier(cell))
                              +"        timestamp="
                              +cell.getTimestamp()+", value="
                              +new String(CellUtil.cloneValue(cell)));
        }
    }
}