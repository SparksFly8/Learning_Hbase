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
    //��������
    public static void init(){
    	// ���� hbase-site.xml �ļ���ʼ�� Configuration ����
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://master:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "master,ceph1,ceph2,ceph3");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try{
        	// ���� Configuration �����ʼ�� Connection ����
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            System.out.println("����HBase�ɹ�.");
        }catch (IOException e){
            System.err.println("����HBaseʧ��.");
        }
    }
    //�ر�����
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
     * ����HBase�ı��л���һ��ϵͳĬ�ϵ�������Ϊ�����������������д�����Ĭ��Ϊput��������б������һ�����ݣ���˴˴����贴��id��
     * @param myTableName ����
     * @param colFamily ������
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
     * ɾ��ָ����
     * @param tableName ����
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
     * �鿴���б�
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
     * ��ĳһ�е�ĳһ�в�������
     * @param tableName ����
     * @param rowKey �м�
     * @param colFamily ������
     * @param col �����������������û�����У��˲�����Ϊ�գ�
     * @param val ֵ
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
     * ɾ������
     * @param tableName ����
     * @param rowKey �м�
     * @param colFamily ������
     * @param col ����
     * @throws IOException
     */
    public static void deleteRow(String tableName,String rowKey,String colFamily,String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());        
        if(col.equals("")) //ɾ��ָ���������������
        	delete.addFamily(colFamily.getBytes());
        else               //ɾ��ָ���е�����
            delete.addColumn(colFamily.getBytes(), col.getBytes());
 
        table.delete(delete);
        table.close();
        close();
    }
    /**
     * �����м�rowkey��������
     * @param tableName ����
     * @param rowKey �м�
     * @param colFamily ������
     * @param col ����
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
     * ��ʽ�����
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