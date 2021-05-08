package databases;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;

import misc.Timer;
import misc.Misc;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.Element;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import enums.ColumnType;
import enums.DatabaseType;
import enums.RequestType;


/**
 * class implementing a database client for Apache HBase
 * 
 * @author Tim Waage
 *
 */
public class DBClientHBase extends DBClient
{
 
	// the used connection
	private Connection connection;
	
	// the used configuration
	private Configuration config;
	
	// the used admin object
	private Admin admin;
	
	// references to the used tables
	HashMap<String, Table> tables = new HashMap<String, Table>();
	
	
	
	/**
	 * Constructor
	 * @param address where to connect to, e.g. 127.0.0.1
	 */
	public DBClientHBase(String _ip) {
			
		// connect
		super(DatabaseType.HBASE, _ip);
		
		// turn off the annoying info messages :-)
		Logger.getRootLogger().setLevel(Level.WARN);
	}
	
	
	/**
	 * connects to an HBase cluster
	 */
	public void connect() {
		
		config = HBaseConfiguration.create();
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
	
	
	
	/**
	 * gets the clients HBaseAdmin instance
	 * @return the clients HBaseAdmin instance
	 */
	public Admin getAdmin() {
		return admin;
	}
	
	
	
	/**
	 * gets the clients configuration
	 * @return the clients configuration
	 */
	public Configuration getConfiguration() {
		return config;
	}
	
	
	
	/**
	 * gets the clients connection
	 * @return the clients connection
	 */
	public Connection getConnection() {
		return connection;
	}
	
	
	private Table getTable(String name) {
		
		if(tables.get(name) != null) return tables.get(name);
		else {
			try {
				Table table = connection.getTable(TableName.valueOf(name));
				tables.put(name, table);
				return table;
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("Unable to get table for set updating: " + name);
				return null;
			}	
		}
	}
	
	
	
	/**
	 * Closes the connection to the HBase cluster
	 */
	public void close() {
				
		try {
			
			if(admin != null) admin.close();
			if (connection != null) connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}


	
	/**
	 * Checks if a certain table exists
	 * @param id id that describes the location of the table 
	 * @return true, if the table exists, false otherwise
	 */
	@Override
	public boolean cipherTableExists(DBLocation id) {		
		try {
			return admin.tableExists(TableName.valueOf((id.getKeyspace().getCipherName() + ":" + id.getTable().getCipherName()).getBytes()));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	
	/**
	 * Checks if a certain table exists
	 * @param id id that describes the location of the table 
	 * @return true, if the table exists, false otherwise
	 */
	@Override
	public boolean cipherKeyspaceExists(DBLocation id) {		
		try {
			NamespaceDescriptor[] nss = admin.listNamespaceDescriptors();
			for(NamespaceDescriptor nd : nss) {
				if(id.getKeyspace().getCipherName().equals(nd.getName())) return true;
			}
						
			return false;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	
	
	/**
	 * Performs interactions (requests) with HBase. Translates request objects to specific API calls. 
	 * @param currentRequest the request object representing the request
	 * @return a result set, containing the results according to the given request
	 */
	@Override
	public databases.Result call() {
		
		timer.reset();
		
		Table table = null; 
		Scan scan = new Scan();
		
		// we assume we only deal with keyspace and table names in cipher form, why else would we use TimDB?
		// Table names are only given, when not creating or dropping a keyspace
		String cipherTableName = null;
		String rowkeyColumnName = null;
		
		if((currentRequest.getType() != RequestType.CREATE_KEYSPACE)&&(currentRequest.getType() != RequestType.DROP_KEYSPACE)) {
			cipherTableName = currentRequest.getId().getKeyspace().getCipherName() + "." + currentRequest.getId().getTable().getCipherName();
			rowkeyColumnName = currentRequest.getId().getTable().getRowkeyColumnName();
		}
		
		switch(currentRequest.getType()) {
		
		case UPDATE_SET:		
			
			table = getTable(cipherTableName);
			
			//get a reference to the table
			/*try {
				table = connection.getTable(TableName.valueOf(cipherTableName));
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("Unable to get table for set updating: " + cipherTableName);
			}*/	
			
			
			
			int n = 1;
			
			String columnName = null;
			
			if((currentRequest.getStringArgs() != null)&&(!currentRequest.getStringArgs().isEmpty())) columnName = currentRequest.getStringArgs().keySet().iterator().next();
			else if((currentRequest.getIntArgs() != null)&&(!currentRequest.getIntArgs().isEmpty())) columnName = currentRequest.getIntArgs().keySet().iterator().next();
			else if((currentRequest.getByteArgs() != null)&&(!currentRequest.getByteArgs().isEmpty())) columnName = currentRequest.getByteArgs().keySet().iterator().next();
							
			scan.addFamily(Bytes.toBytes(columnName));
			
			byte[] rowkey = currentRequest.getId().getRowConditions().get(0).getTermAsByteArray();
			
			scan.setRowPrefixFilter(rowkey);
			try {
				// count the existing entries 
				ResultScanner scanner = table.getScanner(scan);		
				for (Result result = scanner.next(); (result != null); result = scanner.next()) {
					// if(Arrays.equals(r.getId().getRowCondition().getBytes(), result.getRow()))  
						while(result.getValue(Bytes.toBytes(columnName), ("col" + n).getBytes()) != null ) n++;
				}
				
				Put put = new Put(rowkey);
				
				
				if((currentRequest.getStringArgs() != null)&&(!currentRequest.getStringArgs().isEmpty())) put.addColumn(Bytes.toBytes(columnName), Bytes.toBytes("col" + n), currentRequest.getStringArgs().get(columnName).getBytes());
				else if((currentRequest.getIntArgs() != null)&&(!currentRequest.getIntArgs().isEmpty())) put.addColumn(Bytes.toBytes(columnName), Bytes.toBytes("col" + n), Misc.longToBytes(currentRequest.getIntArgs().get(columnName)));
				else if((currentRequest.getByteArgs() != null)&&(!currentRequest.getByteArgs().isEmpty())) put.addColumn(Bytes.toBytes(columnName), Bytes.toBytes("col" + n), currentRequest.getByteArgs().get(columnName));
				
				timer.start();
				table.put(put);
				timer.stop();
				
				scanner.close();
			}
			catch(Exception e) {
				e.printStackTrace();
				System.out.println("Unable to update set");
				return null;
			}
						
			return new ResultHBase(currentRequest, null, timer.getRuntime());
		
		case DROP_TABLE:
			
			// drop table, if exists
			try {
				if(admin.tableExists(TableName.valueOf(cipherTableName))) {
					timer.start();
					admin.disableTable(TableName.valueOf(cipherTableName));
					admin.deleteTable(TableName.valueOf(cipherTableName));
					timer.stop();
				}
				
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Unable to delete table: " + cipherTableName);
			}
			
			return new ResultHBase(currentRequest, null, timer.getRuntime());
			
		case DROP_KEYSPACE:
			
			// drop namespace
			try {
				
				// get and delete all tables within the namespace
				HTableDescriptor[] tds = admin.listTableDescriptorsByNamespace(currentRequest.getId().getKeyspace().getCipherName());
				for(HTableDescriptor td : tds) {
					timer.start();
					admin.disableTable(TableName.valueOf(td.getNameAsString()));
					admin.deleteTable(TableName.valueOf(td.getNameAsString()));
					timer.stop();
				}
				
				admin.deleteNamespace(currentRequest.getId().getKeyspace().getCipherName());
			} catch (IOException e) {
				//e.printStackTrace();
				System.out.println("Unable to delete namespace: " + currentRequest.getId().getKeyspace());
			}
			
			return new ResultHBase(currentRequest, null, timer.getRuntime());	
		
		case CREATE_KEYSPACE:
			
			try {
				timer.start();
				admin.createNamespace(NamespaceDescriptor.create(currentRequest.getId().getKeyspace().getCipherName()).build());
				timer.stop();
			}
			catch(Exception e) {
				System.out.println("Unable to create keyspace: " + currentRequest.getId().getKeyspace().getCipherName());
			}
			
			return new ResultHBase(currentRequest, null, timer.getRuntime());
		
		case CREATE_TABLE:
						
			//create a new table
			HTableDescriptor ht_descriptor = new HTableDescriptor(TableName.valueOf(cipherTableName));
			
			for(String s : currentRequest.getStringArgs().keySet()) if(!s.equals(rowkeyColumnName)) ht_descriptor.addFamily(new HColumnDescriptor(s));
			for(String s : currentRequest.getIntArgs().keySet()) if(!s.equals(rowkeyColumnName)) ht_descriptor.addFamily(new HColumnDescriptor(s));
			for(String s : currentRequest.getByteArgs().keySet()) if(!s.equals(rowkeyColumnName)) ht_descriptor.addFamily(new HColumnDescriptor(s));
					
			try {
				timer.start();
				admin.createTable(ht_descriptor);
				timer.stop();
			} catch (IOException e) {
				//e.printStackTrace();
				//System.out.println("Unable to create table: " + tablename);
			}
			
			return new ResultHBase(currentRequest, null, timer.getRuntime());
			
		case INSERT: 
			
			//get a reference to the table
			table = getTable(cipherTableName);
			
			
			Put put = null;
			
			if(currentRequest.getByteArgs().get(rowkeyColumnName) != null) put = new Put(currentRequest.getByteArgs().get(currentRequest.getId().getTable().getRowkeyColumnName()));
			else if(currentRequest.getStringArgs().get(rowkeyColumnName) != null) put = new Put(currentRequest.getStringArgs().get(rowkeyColumnName).getBytes()); 
			else if(currentRequest.getIntArgs().get(rowkeyColumnName) != null) put = new Put(Bytes.toBytes(currentRequest.getIntArgs().get(rowkeyColumnName))); 
																					
			for(String s : currentRequest.getStringArgs().keySet()) if(!s.equals(rowkeyColumnName)) put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"), currentRequest.getStringArgs().get(s).getBytes()); 
			for(String s : currentRequest.getIntArgs().keySet()) if(!s.equals(rowkeyColumnName)) put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"), Misc.longToBytes(currentRequest.getIntArgs().get(s)));
			for(String s : currentRequest.getByteArgs().keySet()) if(!s.equals(rowkeyColumnName)) put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"), currentRequest.getByteArgs().get(s)); 

			int setElementsCounter = 0;
			
			// experimental
			for(String s : currentRequest.getStringSets().keySet()) {
				
				HashSet<ByteBuffer> bytesSet = Misc.StringHashSet2ByteBufferHashSet(currentRequest.getStringSets().get(s));
				for(ByteBuffer bb : bytesSet) {
					setElementsCounter++;
					put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"+setElementsCounter), bb.array());
				}				
			}
			
			// experimental
			for(String s : currentRequest.getIntSets().keySet()) {
				
				HashSet<ByteBuffer> bytesSet = Misc.LongHashSet2ByteBufferHashSet(currentRequest.getIntSets().get(s));
				for(ByteBuffer bb : bytesSet) {
					setElementsCounter++;
					put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"+setElementsCounter), bb.array());
				}				
			}

			for(String s : currentRequest.getByteSets().keySet()) {
	
				HashSet<ByteBuffer> bytesSet = currentRequest.getByteSets().get(s);
				for(ByteBuffer bb : bytesSet) {
					setElementsCounter++;
					put.addColumn(Bytes.toBytes(s), Bytes.toBytes("col"+setElementsCounter), bb.array());
				}				
			}
			
			try {
				if(!put.isEmpty()) {
					timer.start();
					table.put(put);
					timer.stop();
					return new ResultHBase(currentRequest, null, timer.getRuntime());
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Unable to put data into " + currentRequest.getId().toString());
			}	
				
			// there's nothing to return here
			return null;
			
		case READ:
			
			RowFilter rowFilter1 = null;
			
			//get a reference to the table
			table = getTable(cipherTableName);
			
			// add selected columns
			for(String column : currentRequest.getId().getColumns()) 
				if(!column.equals(currentRequest.getId().getTable().getRowkeyColumnName())) {
					scan.addFamily(column.getBytes());
					
				}
			
			// add condition columns
			if(currentRequest.getId().getRowConditions() != null){
				for(RowCondition rc: currentRequest.getId().getRowConditions()) {
					if((rc.getComparator().equals("="))||(rc.getComparator().equals(">"))||(rc.getComparator().equals("<")))
						if(!rc.getColumnName().equals(currentRequest.getId().getTable().getRowkeyColumnName())) {
							scan.addFamily(rc.getColumnName().getBytes());
						}
						else {
							rowFilter1 = new RowFilter(CompareOp.EQUAL, new BinaryComparator(rc.getByteTerm()));	
							}
							

				}
			}
			
			// always add IV column
			scan.addFamily(currentRequest.getId().getTable().getIVcolumnName().getBytes());
			
			// add row conditions
			if((currentRequest.getId().getRowConditions() != null)&&(currentRequest.getId().getRowConditions().size() > 0)) {
				
				FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				
				for(RowCondition rc : currentRequest.getId().getRowConditions()) {
					
					if(!rc.getComparator().equals("#")) { // only add non SE row conditions
					
						//CompareOp com = null;
						CompareOp com = null;
						if(rc.getComparator().equals("=")) com = CompareOp.EQUAL;
						if(rc.getComparator().equals(">")) com = CompareOp.GREATER;
						if(rc.getComparator().equals("<")) com = CompareOp.LESS;
					
						
						
						SingleColumnValueFilter filter = null;
						if(rc.getType() == ColumnType.STRING) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, rc.getStringTerm().getBytes());
						if(rc.getType() == ColumnType.INTEGER) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, Bytes.toBytes(rc.getLongTerm()));
						if(rc.getType() == ColumnType.BYTE) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, rc.getByteTerm());
				    
						filterlist.addFilter(filter);
					}
				}
				
				if(rowFilter1 != null) filterlist.addFilter(rowFilter1);
				
				if(!filterlist.getFilters().isEmpty()) scan.setFilter(filterlist);
				
			}
			
			try {
				
				timer.start();
				ResultScanner tmp = table.getScanner(scan);
				timer.stop();
				return new ResultHBase(currentRequest, tmp, timer.getRuntime());
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Error during HBase read.");
				return null;
			}		
				
		case READ_WITHOUT_IV:
	
			RowFilter rowFilter2 = null;
			
			//get a reference to the table
			table = getTable(cipherTableName);
			
			// add selected columns
			for(String column : currentRequest.getId().getColumns()) 
				if(!column.equals(currentRequest.getId().getTable().getRowkeyColumnName())) {
					scan.addFamily(column.getBytes());
					
				}
	
			// add condition columns
			if(currentRequest.getId().getRowConditions() != null){
				for(RowCondition rc: currentRequest.getId().getRowConditions()) {
					if((rc.getComparator().equals("="))||(rc.getComparator().equals(">"))||(rc.getComparator().equals("<")))
						if(!rc.getColumnName().equals(currentRequest.getId().getTable().getRowkeyColumnName())) {
							scan.addFamily(rc.getColumnName().getBytes());
							
						}
						else {
							
							rowFilter2 = new RowFilter(CompareOp.EQUAL, new BinaryComparator(rc.getByteTerm()));	
						}
					

				}
			}
			
			// add row conditions
			if((currentRequest.getId().getRowConditions() != null)&&(currentRequest.getId().getRowConditions().size() > 0)) {
		
				FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
				for(RowCondition rc : currentRequest.getId().getRowConditions()) {
			
					if(!rc.getComparator().equals("#") && (!rc.getColumnName().equals(currentRequest.getId().getTable().getRowkeyColumnName()))) { // only add non SE row conditions
						
						CompareOp com = null;
						if(rc.getComparator().equals("=")) com = CompareOp.EQUAL;
						if(rc.getComparator().equals(">")) com = CompareOp.GREATER;
						if(rc.getComparator().equals("<")) com = CompareOp.LESS;
			
						SingleColumnValueFilter filter = null;
						if(rc.getType() == ColumnType.STRING) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, rc.getStringTerm().getBytes());
						if(rc.getType() == ColumnType.INTEGER) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, Misc.longToBytes(rc.getLongTerm()));
						if(rc.getType() == ColumnType.BYTE) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, rc.getByteTerm());
		    
						filterlist.addFilter(filter);
					}
				}
		
				if(rowFilter2 != null) filterlist.addFilter(rowFilter2);
				
				if(!filterlist.getFilters().isEmpty()) scan.setFilter(filterlist);
				
			}
	
			try {
				timer.start();
				ResultScanner tmp = table.getScanner(scan);
				timer.stop();
				return new ResultHBase(currentRequest, tmp, timer.getRuntime());
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Error during HBase read without IV.");
				return null;
			}		
			
		
		case READ_WITH_SET_CONDITION:
			
			//get a reference to the table
			table = getTable(cipherTableName);
			
			// add columns
			for(String column : currentRequest.getId().getColumns()) if(!column.equals(currentRequest.getId().getTable().getRowkeyColumnName())) scan.addFamily(column.getBytes());
	
			// add condition columns
						if(currentRequest.getId().getRowConditions() != null){
							for(RowCondition rc: currentRequest.getId().getRowConditions()) {
								if((rc.getComparator().equals("="))||(rc.getComparator().equals(">"))||(rc.getComparator().equals("<")))
									if(!rc.getColumnName().equals(currentRequest.getId().getTable().getRowkeyColumnName()))
										scan.addFamily(rc.getColumnName().getBytes());
								
//								ColumnState cs = currentRequest.getId().getTable().getColumnByName(rc.getColumnName());
//							
//								if(!rc.getColumnName().equals(currentRequest.getId().getTable().getRowkeyColumnName())) {
//								
//									if(cs.isEncrypted()) {
//										if(cs.getType() == ColumnType.STRING) {
//											scan.addFamily(cs.getCSEname().getBytes());
//											System.out.println("Added: " + cs.getCSEname());
//										}
//										if(cs.getType() == ColumnType.INTEGER) {
//											scan.addFamily(cs.getCOPEname().getBytes());
//											System.out.println("Added: " + cs.getCOPEname());
//										}
//									}
//									else {
//										scan.addFamily(cs.getPlainName().getBytes());
//										System.out.println("Added: " + cs.getPlainName());
//									}
//								}			
							}
						}
			
			// add row conditions
			if((currentRequest.getId().getRowConditions() != null)&&(currentRequest.getId().getRowConditions().size() > 0)) {
		
				FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		
				for(RowCondition rc : currentRequest.getId().getRowConditions()) {
			
						CompareOp com = CompareOp.EQUAL;
						
						SingleColumnValueFilter filter = null;
						if(rc.getType() == ColumnType.STRING) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, rc.getStringTerm().getBytes());
						if(rc.getType() == ColumnType.INTEGER) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, Misc.longToBytes(rc.getLongTerm()));
						if(rc.getType() == ColumnType.BYTE) filter = new SingleColumnValueFilter(rc.getColumnName().getBytes(), Bytes.toBytes("col"), com, rc.getByteTerm());
		    
						filterlist.addFilter(filter);
					
				}
		
				if(!filterlist.getFilters().isEmpty()) scan.setFilter(filterlist);
				//scan.setRowPrefixFilter(r.getId().getRowConditions().get(0).getTermAsByteArray());
			}
	
			try {
				timer.start();
				ResultScanner tmp = table.getScanner(scan);
				timer.stop();
				return new ResultHBase(currentRequest, tmp, timer.getRuntime());
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Error during HBase read with set condition.");
				return null;
			}	
			
			
		case UPDATE_VALUE:
			
			//get a reference to the table
			try {
				table = getTable(cipherTableName);
				
				//table = connection.getTable(TableName.valueOf(cipherTableName));
				
				Put update = new Put(currentRequest.getId().getRowConditions().get(0).getTermAsByteArray());
				
				for(String s : currentRequest.getStringArgs().keySet()) update.addColumn(s.getBytes(), Bytes.toBytes("col"), currentRequest.getStringArgs().get(s).getBytes()); 
				for(String s : currentRequest.getIntArgs().keySet()) update.addColumn(s.getBytes(), Bytes.toBytes("col"), Misc.longToBytes(currentRequest.getIntArgs().get(s)));
				for(String s : currentRequest.getByteArgs().keySet()) update.addColumn(s.getBytes(), Bytes.toBytes("col"), currentRequest.getByteArgs().get(s)); 
			
				timer.start();
				table.put(update);
				timer.stop();
				return new ResultHBase(currentRequest, null, timer.getRuntime());
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("Unable to update: " + currentRequest.getId().toString());
			}	
						
			return null;	
			
		default:
			break;		
		
		}
		
		return null;
	}


	@Override
	public void initializeFromXMLElement(Element data) {
		// TODO Auto-generated method stub
		
	}


	@Override
	/**
	 * Removes the RND layer from an encrypted onion column
	 * @param cs the column
	 * @param onion "DET" for removing the RND layer above the DET layer, "OPE" analogous
	 */
	public void removeRNDLayer(ColumnState cs, String onion) {
		
		Timer t = new Timer();
		t.start();
		
		// retrieve column key 
		byte[] key = cs.getKey();
				
		// retrieve rowkeys, IVs and column content 
		TableState table = cs.getTable();
				
		String columnName = null;
		if(onion.equals("DET")) columnName = cs.getCDETname();
		if(onion.equals("OPE")) columnName = cs.getCOPEname();
		
		Table hTable = null;
		Scan scan = new Scan();
		ResultScanner scanner = null;
		
		try {
			hTable = connection.getTable(TableName.valueOf(table.getKeyspace().getCipherName() + "." + table.getCipherName()));
			
			scan.addFamily(table.getIVcolumnName().getBytes());
			scan.addFamily(columnName.getBytes());
			
			scanner = hTable.getScanner(scan);
					
			// for all rows
			for (org.apache.hadoop.hbase.client.Result result = scanner.next(); result != null; result = scanner.next()) {
				
				// RND layer decrypt and remove
				byte[] encryptedValue = result.getValue(columnName.getBytes(), Bytes.toBytes("col"));
				byte[] iv = result.getValue(table.getIVcolumnName().getBytes(), Bytes.toBytes("col"));
				byte[] rowkey = result.getRow();
				byte[] decryptedValue = cs.getRNDScheme().decrypt(encryptedValue, iv);
				
				//System.out.println("Row updated with: " + Misc.bytesToLong(decryptedValue));
				
				// write back decrypted column content
				Put put = new Put(rowkey);
				put.addColumn(columnName.getBytes(), "col".getBytes(), decryptedValue);		
				if(!put.isEmpty()) hTable.put(put);					
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error during RND layer removal of " + table.getKeyspace().getCipherName() + ":" + table.getCipherName() + "." + cs.getPlainName());
		}		
				
		// flag setzen
		if(onion.equals("DET")) cs.setRNDoverDETStrippedOff(true);
		if(onion.equals("OPE")) cs.setRNDoverOPEStrippedOff(true);
		
		t.stop();
		System.out.println("RND layer removed from column \"" + cs.getPlainName() + "\" (" + t.getRuntimeAsString() + ")");
				
	}


	

	@Override
	
		
	public PreparedStatement registerStatement(String label, String query) {
		
		return null;
		
	}
	
	


	
		
}