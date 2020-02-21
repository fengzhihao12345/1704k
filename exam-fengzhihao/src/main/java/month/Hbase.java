package month;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbase {
	private static Configuration conf;
	private static Connection conn;
	public static Connection get_connection() throws Exception{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","192.168.190.136");
		conf.set("hbase.zookeeper.property.ClientPort","2181");
		conn =  ConnectionFactory.createConnection(conf);
		return conn;
	}
	public static boolean create_table() throws Exception{
		Admin admin = get_connection().getAdmin();
		HTableDescriptor desc  = new HTableDescriptor(TableName.valueOf("hbasetest"));
		HColumnDescriptor clo = new HColumnDescriptor("info");
		desc.addFamily(clo);
		admin.createTable(desc);
		conn.close();
		return true;
	}
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Table table = get_connection().getTable(TableName.valueOf("hbasetest"));
		List<Put> list = new ArrayList<Put>();
		Put e = new Put(Bytes.toBytes("0_01"));
		e.add(Bytes.toBytes("deptinfo"), Bytes.toBytes("name"), Bytes.toBytes("ceo"));
		e.add(Bytes.toBytes("Childdeptinfo"), Bytes.toBytes("child1"), Bytes.toBytes("1_02"));
		e.add(Bytes.toBytes("Childdeptinfo"), Bytes.toBytes("child2"), Bytes.toBytes("1_05"));
		
		Put e1 = new Put(Bytes.toBytes("1_02"));
		e.add(Bytes.toBytes("deptinfo"), Bytes.toBytes("name"), Bytes.toBytes("develop"));
		e.add(Bytes.toBytes("sup"), Bytes.toBytes("name"), Bytes.toBytes("0_01"));
		e.add(Bytes.toBytes("Childdeptinfo"), Bytes.toBytes("child1"), Bytes.toBytes("2_03"));
		e.add(Bytes.toBytes("Childdeptinfo"), Bytes.toBytes("child2"), Bytes.toBytes("2_04"));
		
		Put e3 = new Put(Bytes.toBytes("2_03"));
		e.add(Bytes.toBytes("deptinfo"), Bytes.toBytes("name"), Bytes.toBytes("develop1"));
		e.add(Bytes.toBytes("sup"), Bytes.toBytes("name"), Bytes.toBytes("1_02"));
		
		Put e4 = new Put(Bytes.toBytes("2_04"));
		e.add(Bytes.toBytes("deptinfo"), Bytes.toBytes("name"), Bytes.toBytes("develop2"));
		e.add(Bytes.toBytes("sup"), Bytes.toBytes("name"), Bytes.toBytes("1_02"));
		
		Put e2 = new Put(Bytes.toBytes("1_05"));
		e.add(Bytes.toBytes("deptinfo"), Bytes.toBytes("name"), Bytes.toBytes("web"));
		e.add(Bytes.toBytes("sup"), Bytes.toBytes("name "), Bytes.toBytes("0_01"));
		e.add(Bytes.toBytes("Childdeptinfo"), Bytes.toBytes("child1"), Bytes.toBytes("2_06"));
		e.add(Bytes.toBytes("Childdeptinfo"), Bytes.toBytes("child2"), Bytes.toBytes("2_07"));
		
		Put e5 = new Put(Bytes.toBytes("2_06"));
		e.add(Bytes.toBytes("deptinfo"), Bytes.toBytes("name"), Bytes.toBytes("web1"));
		e.add(Bytes.toBytes("sup"), Bytes.toBytes("name"), Bytes.toBytes("1_05"));
		
		Put e6 = new Put(Bytes.toBytes("2_07"));
		e.add(Bytes.toBytes("deptinfo"), Bytes.toBytes("name"), Bytes.toBytes("web2"));
		e.add(Bytes.toBytes("sup"), Bytes.toBytes("name"), Bytes.toBytes("1_05"));
		list.add(e);
		list.add(e1);
		list.add(e2);
		list.add(e3);
		list.add(e4);
		list.add(e5);
		list.add(e6);
		table.put(list);
		get_connection().close();
	}
}
