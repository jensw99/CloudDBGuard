import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.datastax.oss.driver.api.core.cql.Row;

import databases.HBaseSetFilter;
import databases.ResultHBase;
import databases.RowCondition;
import enums.ColumnType;
import misc.Misc;
import misc.Timer;

public class DBHBaseUnencrypted {
	// the used connection
	private Connection connection;
	private Timer timer;
	
	// the used configuration
	private Configuration config;
	
	// the used admin object
	private Admin admin;
	
	public void dropKeyspace(String keyspace) {
		try {
			
			// get and delete all tables within the namespace
			TableName[] tds =  admin.listTableNamesByNamespace(keyspace);
			for(TableName td : tds) {
				timer.start();
				admin.disableTable(td);
				admin.deleteTable(td);
				timer.stop();
			}
			
			admin.deleteNamespace(keyspace);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public DBHBaseUnencrypted(String ip) {
		timer = new Timer();
		
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ip);
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.rpc.timeout", "20000");
		config.set("hbase.client.scanner.timeout.period", "40000");
		
		System.out.println("hbase config="+config);
		try {
			connection = ConnectionFactory.createConnection(config);
			admin = connection.getAdmin();
			Misc.printStatus("Connected to HBase cluster");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void createEnronKeyspace() {
		try {
			admin.createNamespace(NamespaceDescriptor.create("enron_unencrypted").build());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	public void deleteTable(String table) {
		try {
			admin.disableTable(TableName.valueOf(table));
			admin.deleteTable(TableName.valueOf(table));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public void createEnronTable() {
		//create a new table
		TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf("enron_unencrypted","enron"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("id"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("sender"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("receiver"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cc"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("bcc"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("subject"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("body"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("path"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("year"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("month"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("day"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("size"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("timestamp"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xcc"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xfolder"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xorigin"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("mimeversion"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xbcc"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xfilename"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xto"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xfrom"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cte"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("xte"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("contenttype"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("writer"))
                .build();
		
				
		try {
			admin.createTable(desc);
		} catch (IOException e) {
			//e.printStackTrace();
			//System.out.println("Unable to create table: " + tablename);
		}
		
	}
	public void dropEnronKeyspace() {
		try {
			admin.disableTable(TableName.valueOf("enron_unencrypted","enron"));
			admin.deleteTable(TableName.valueOf("enron_unencrypted","enron"));
			admin.deleteNamespace("enron_unencrypted");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public long insertRow(HashMap <String, String> rowStrings, HashMap<String, Long> rowLongs, HashMap<String, HashSet<String>> rowStringSets) {
		timer.reset();
		Put put = null;
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf("enron_unencrypted", "enron"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
 
		put = new Put(Bytes.toBytes(rowStrings.get("id")));																	
		for(String s : rowStrings.keySet()) if(!s.equals("id"))  put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"), rowStrings.get(s).getBytes()); 
		for(String s : rowLongs.keySet()) put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"), Misc.longToBytes(rowLongs.get(s))); 

		int setElementsCounter = 0;
		
		// experimental
		for(String s : rowStringSets.keySet()) {
			
			HashSet<ByteBuffer> bytesSet = Misc.StringHashSet2ByteBufferHashSet(rowStringSets.get(s));
			for(ByteBuffer bb : bytesSet) {
				setElementsCounter++;
				put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"+setElementsCounter), bb.array());
			}				
		}
		
		timer.start();
		try {
			if(!put.isEmpty()) {
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		timer.stop();
		
		return timer.getRuntime();
			

	}
	public long query(String[] columns, String keyspace, String _table, String[] conditions) {
		timer.reset();
		
		Scan scan = new Scan();
		HashMap<String, String> search = new HashMap<String, String>();
		
		
		// add selected columns
		/*
		for(String column : columns) 
			if(!column.equals("id")) {
				scan.addFamily(column.getBytes());
				
			}*/
		
		
		
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(keyspace, _table));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		
		
		// add row conditions

			
		FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		String operator = "";
		for(String rc : conditions) {
			if (rc.split("=").length > 1) operator = "=";
			if (rc.split("#").length > 1) operator = "#";
			if (rc.split("<").length > 1) operator = "<";
			if (rc.split(">").length > 1) operator = ">";
			
			String column = rc.split(operator)[0];
			String value = rc.split(operator)[1];
			scan.addFamily(column.getBytes());
			
			
			if(operator != "#") { // only add non SE row conditions
			
				//CompareOp com = null;
				CompareOperator com = null;
				if(operator == "=") com = CompareOperator.EQUAL;
				if(operator == ">") com = CompareOperator.GREATER;
				if(operator == "<") com = CompareOperator.LESS;
				
				boolean textCol = false;
				if(column.equals("sender") || column.equals("body") || column.equals("subject") || column.equals("writer")) textCol = true;
				
				boolean setCol = false;
				if(column.equals("receiver")) setCol = true;
			
				
				if(setCol) {
					for (String v : value.split(",")) {
						HBaseSetFilter filter = new HBaseSetFilter(column.getBytes(), v.trim().getBytes());
						filterlist.addFilter(filter);
					}

				}else {
					SingleColumnValueFilter filter = null;
					if(textCol) filter = new SingleColumnValueFilter(column.getBytes(), "col".getBytes(), com, rc.split(operator)[1].getBytes());
					else filter = new SingleColumnValueFilter(column.getBytes(), "col".getBytes(), com, Misc.longToBytes(Long.valueOf(value)));
					filterlist.addFilter(filter);
				}
				
			}else {
				String[] SEValue = rc.split(operator);
				search.put(SEValue[0], SEValue[1]);
			}
			
			
			
		}
		if(!filterlist.getFilters().isEmpty()) scan.setFilter(filterlist);
		ResultScanner tmp = null;
		try {
			
			timer.start();
			tmp = table.getScanner(scan);
			timer.stop();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(search.size() > 0) {
			int counter = 0;
			for(Result r : tmp) {
				boolean in = true;
				for (Map.Entry<String, String> e : search.entrySet()) {
					if (!lookup(Bytes.toString(r.getValue(e.getKey().getBytes(), "col".getBytes())), e.getValue())) {
						in = false;
					}
				}
				if (in) counter++;
			}
			System.out.println(counter);
			ClientQueryUnencrypted.counter += counter;
		}else {
			int counter = 0;
			for(Result r : tmp) counter ++;
			System.out.println(counter);
			ClientQueryUnencrypted.counter += counter;
		}
		return timer.getRuntime();
	}
	public boolean lookup(String input, String searchWord) {
		boolean result = false;
		for(String s : Misc.getWords(input)) {
			if(s.equals(searchWord)) result = true;
		}
		
		return result;
	}
	public void close() {
		try {
			admin.close();
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
