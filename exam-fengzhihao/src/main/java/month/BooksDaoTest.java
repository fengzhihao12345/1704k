package month;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;



public class BooksDaoTest {
	public static Connection get_conn() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","192.168.190.136");
		conf.set("hbase.zookeeper.property.ClientPort","2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		return conn;
	}
	public static void get_close() throws Exception{
		get_conn().close();
	}
	public static void publisherFilterTest() throws Exception, Exception{
		Table table =  get_conn().getTable(TableName.valueOf("hbase_books"));
		SingleColumnValueFilter sing = new  SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("publisher"), CompareOp.EQUAL, Bytes.toBytes("Signet Book"));
		Scan scan = new Scan();
		scan.setFilter(sing);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			String row = Bytes.toString(result.getRow());
			String value = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("book_title")));
			System.out.println(String.format("%s:%s",row,value));
		}
		get_close();
	}
	public static void nameIncludeTest() throws Exception, Exception{
		Table table = get_conn().getTable(TableName.valueOf("hbase_books"));
		SingleColumnValueFilter sing = new  SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("book_author"), CompareOp.EQUAL,new SubstringComparator("Die"));
		ValueFilter va =  new ValueFilter(CompareOp.EQUAL,new SubstringComparator("Die"));
		Scan scan = new Scan();
		FilterList list = new FilterList(sing,va);
		scan.setFilter(list);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			String value = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("book_author")));
			String values = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("book_title")));
			System.out.println(String.format("%s:%s",value,values));
		}
		get_close();
	}
	public static void countBooksByYearTest() throws Exception, Exception{
		int sum = 0;
		Table table = get_conn().getTable(TableName.valueOf("hbase_books"));
		SingleColumnValueFilter sing = new  SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("data"), CompareOp.GREATER_OR_EQUAL,new SubstringComparator("1990"));
		SingleColumnValueFilter sings = new  SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("data"), CompareOp.LESS_OR_EQUAL,new SubstringComparator("1999"));
		SingleColumnValueFilter valueFilterss = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("Book_Author"), CompareOp.EQUAL,Bytes.toBytes("Kathleen Duey"));
		Scan scan = new Scan();
		FilterList list = new FilterList(sing,sings,valueFilterss);
		scan.setFilter(list);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			sum++;
		}
		System.out.println(sum);
		get_close();
	}
	
	public static void main(String[] args) throws Exception {
		nameIncludeTest();
	}
}
