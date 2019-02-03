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
		String tableName = "2018AAAI_Papers";  //���ݿ����
		String colFamily1 = "paper_info";      //��һ���д�
		String colFamily2 = "creator_info";    //�ڶ����д�
		String xlsx_Path = "C:/Users/Administrator/Desktop/whole_data.xlsx";
		String year = "2018";                  //��ȡ���������
		//readXlsx();
		init();
//		deleteTable(tableName);
		createTable(tableName,new String[]{colFamily1,colFamily2});
		for(int ColNum=2;ColNum<=4;ColNum++)   //���ζ�ȡxlsx�ļ��ĵ�ColNum��(��0����)
			xlsx2HBase(tableName,colFamily1,xlsx_Path,ColNum,year);
		for(int ColNum=5;ColNum<=46;ColNum++)  //����creator��Ϣ����creator_info�д���
			xlsx2HBase(tableName,colFamily2,xlsx_Path,ColNum,year);
		listTables();
    	close();
	}
	public static void xlsx2HBase(String tableName,String colFamily,String xlsx_Path,int xlsx_ColNum,String year) throws IOException{
		//��ȡHBase���ݿ��
		Table table = connection.getTable(TableName.valueOf(tableName)); 
		//��ȡxlsx�ļ�ĳһ������
		InputStream inputstream = new FileInputStream(xlsx_Path);
		XSSFWorkbook workbook = new XSSFWorkbook(inputstream);  //����һ�����������Ѷ�ȡ�������ݷ�������	
		XSSFSheet sheet = workbook.getSheetAt(0);               //��ȡ�������еĵ�1��sheet(��0����)
		int totalRows = sheet.getPhysicalNumberOfRows();        //��ȡ��sheet���ж���������
		String cell;                                            //xlsx��Ԫ����Ϣ
		String rowKey;                                          //HBase�е�rowKey
		Put put;                                                //HBase�е�putʵ��
		String header = sheet.getRow(0).getCell(xlsx_ColNum).toString(); //��ȡ���б�ͷ��Ϣ
		for(int i=sheet.getFirstRowNum()+1;i<totalRows;i++){        //xlsx��������0����
			cell = sheet.getRow(i).getCell(xlsx_ColNum).toString(); //��ȡxlsx��i�е�xlsx_ColNum��Ԫ����Ϣ
			if(cell.equals("0")) continue;                          //�����Ԫ��ֵ��"0"��ʾ������Ӹ���Ϣ
			rowKey = year+String.format("%04d", i);                 //������ݺ���ֵƴ�ӳ��ַ����γ�rowKey
			put = new Put(rowKey.getBytes());                   //����putʵ��
	        put.addColumn(colFamily.getBytes(), header.getBytes(), cell.getBytes());
	        table.put(put);                                         //��HBase�е����ݿ���������
	        System.out.println("��"+rowKey+"��"+header+"�в���ɹ�.");
		}      
		//�ر����ݿ��
        table.close();  
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
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("���������ݿ���Ѿ�����!");
        }else {
        	TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for(String str:colFamily){
            	ColumnFamilyDescriptor columnfamily = ColumnFamilyDescriptorBuilder.of(str);
            	tableDescriptor.setColumnFamily(columnfamily);
            }
            admin.createTable(tableDescriptor.build());
            System.out.println("�ɹ�����"+myTableName+"���ݿ��");
        }
    }
    /**
     * ɾ��ָ����
     * @param tableName ����
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        System.out.println("ɾ�����ݿ��"+tableName+"�ɹ�.");
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
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }
    /**
     * �鿴���б�
     * @throws IOException
     */
    public static void listTables() throws IOException {
    	System.out.println("�г����ݿ������:");
        TableName tablenames[] = admin.listTableNames();
        for(TableName tablename:tablenames){
        	System.out.println(tablename.getNameAsString());
        }
    }
}
