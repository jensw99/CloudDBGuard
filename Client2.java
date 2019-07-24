import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Client2 
{
	static Connection connection;
	static Configuration hBaseConfig;
	static Admin admin;
	
	public static void main(String[] args) 
	{	
		hBaseConfig = HBaseConfiguration.create();
		
		System.out.println("hbase config="+hBaseConfig);
		
		try {
			connection = ConnectionFactory.createConnection(hBaseConfig);
			admin = connection.getAdmin();
			System.out.println("Connected to HBase cluster");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
