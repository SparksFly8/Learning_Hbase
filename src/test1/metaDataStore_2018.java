package test1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
public class metaDataStore_2018 {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    static String xlsxPath = "C:/Users/Administrator/Desktop/whole_data.xlsx";
	public static void main(String[] args) throws IOException{
		String tableName = "2018AAAI_Papers";  //数据库表名
		String colFamily1 = "paper_info";      //第一个列簇
		String colFamily2 = "creator_info";    //第二个列簇
		String xlsx_Path = "C:/Users/Administrator/Desktop/whole_data.xlsx";
		String year = "2018";                  //爬取的论文年份
		//readXlsx();
		init();
//		deleteTable(tableName);
		createTable(tableName,new String[]{colFamily1,colFamily2});
		for(int ColNum=2;ColNum<=4;ColNum++)   //依次读取xlsx文件的第ColNum列(从0计数)
			xlsx2HBase(tableName,colFamily1,xlsx_Path,ColNum,year);
		for(int ColNum=5;ColNum<=46;ColNum++)  //对于creator信息放入creator_info列簇中
			xlsx2HBase(tableName,colFamily2,xlsx_Path,ColNum,year);
		listTables();
    	close();
	}
	public static void xlsx2HBase(String tableName,String colFamily,String xlsx_Path,int xlsx_ColNum,String year) throws IOException{
		//获取HBase数据库表
		Table table = connection.getTable(TableName.valueOf(tableName)); 
		//读取xlsx文件某一列数据
		InputStream inputstream = new FileInputStream(xlsx_Path);
		XSSFWorkbook workbook = new XSSFWorkbook(inputstream);  //创建一个工作簿，把读取的流数据放入其中	
		XSSFSheet sheet = workbook.getSheetAt(0);               //获取工作簿中的第1个sheet(从0计数)
		int totalRows = sheet.getPhysicalNumberOfRows();        //获取该sheet中有多少行数据
		String cell;                                            //xlsx单元格信息
		String rowKey;                                          //HBase中的rowKey
		Put put;                                                //HBase中的put实例
		String header = sheet.getRow(0).getCell(xlsx_ColNum).toString(); //获取该列表头信息
		for(int i=sheet.getFirstRowNum()+1;i<totalRows;i++){        //xlsx表行数从0计数
			cell = sheet.getRow(i).getCell(xlsx_ColNum).toString(); //获取xlsx第i行第xlsx_ColNum单元格信息
			if(cell.equals("0")) continue;                          //如果单元格值是"0"表示无需添加该信息
			rowKey = year+String.format("%04d", i);                 //根据年份和行值拼接成字符串形成rowKey
			put = new Put(rowKey.getBytes());                   //创建put实例
	        put.addColumn(colFamily.getBytes(), header.getBytes(), cell.getBytes());
	        table.put(put);                                         //向HBase中的数据库表插入数据
	        System.out.println("第"+rowKey+"行"+header+"列插入成功.");
		}      
		//关闭数据库表
        table.close();  
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
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("创建的数据库表已经存在!");
        }else {
        	TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for(String str:colFamily){
            	ColumnFamilyDescriptor columnfamily = ColumnFamilyDescriptorBuilder.of(str);
            	tableDescriptor.setColumnFamily(columnfamily);
            }
            admin.createTable(tableDescriptor.build());
            System.out.println("成功创建"+myTableName+"数据库表");
        }
    }
    /**
     * 删除指定表
     * @param tableName 表名
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        System.out.println("删除数据库表"+tableName+"成功.");
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
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }
    /**
     * 查看已有表
     * @throws IOException
     */
    public static void listTables() throws IOException {
    	System.out.println("列出数据库表如下:");
        TableName tablenames[] = admin.listTableNames();
        for(TableName tablename:tablenames){
        	System.out.println(tablename.getNameAsString());
        }
    }
}
