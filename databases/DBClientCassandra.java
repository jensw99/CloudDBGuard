package databases;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

import org.jdom2.Element;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.protocol.internal.ProtocolConstants.ConsistencyLevel;

import misc.Misc;
import misc.Timer;


import enums.ColumnType;
import enums.DatabaseType;
import enums.RequestType;

/**
 * class implementing a database client for Apache Cassandra
 * 
 * @author Tim Waage
 *
 */
public class DBClientCassandra extends DBClient {

	// null indicates that there is currently no connection
	private CqlSession session;
	
	private HashMap<String, PreparedStatement> preparedStatements;
	
	/**
	 * Constructor
	 * @param address where to connect to, e.g. 127.0.0.1
	 */
	public DBClientCassandra(String _ip) {
		
		// connect
		super(DatabaseType.CASSANDRA, _ip);	
		
		preparedStatements = new HashMap<String, PreparedStatement>();
	}
	
	
	/**
	 * Connects to a Cassandra cluster
	 * 
	 * @param ip the target node (IP address)
	 */
	public void connect() {
		
		session = CqlSession.builder()
			    .addContactPoint(new InetSocketAddress(ip, 9042))
			    .withLocalDatacenter("datacenter1")
			    .build();
		Misc.printStatus("Connected to Cassandra cluster: " + session.getMetadata().getClusterName());
		//for ( Host host : metadata.getAllHosts() ) System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
	}
	
	
	
	/**
	 * gets the session of a connection
	 * @return the session of a connection
	 */
	public CqlSession getSession() {
		
		return session;
	}

	
	
	/**
	 * gets the clusters metadata
	 * @return the clusters metadata
	 */
	public Metadata getMetadata() {
		
		return session.getMetadata();
	}
	
	
	
	/**
	 * Closes the connection to the Cassandra cluster
	 */
	public void close() {
		session.close();
	}

	
	
	
	
	

	/**
	 * Checks if a certain table exists
	 * @param id id that describes the location of the table 
	 * @return true, if the table exists, false otherwise
	 */
	@Override
	public boolean cipherTableExists(DBLocation id) {
		
		Optional<KeyspaceMetadata> ks = session.getMetadata().getKeyspace(id.getKeyspace().getCipherName());
		if(!ks.isPresent()) return false;
		else {
			if(ks.get().getTable(id.getTable().getCipherName()) == null) return false;
			else return true;
		}
	}
	

	
	/**
	 * Checks if a certain keyspace exists
	 * @param id id that describes the location of the keyspace 
	 * @return true, if the keyspace exists, false otherwise
	 */
	@Override
	public boolean cipherKeyspaceExists(DBLocation id) {
		
		Optional<KeyspaceMetadata> ks = session.getMetadata().getKeyspace(id.getKeyspace().getCipherName());
		if(!ks.isPresent()) return false;
		else return true;	
	}
	
	
	
	/**
	 * Performs interactions (requests) with Cassandra. Translates request objects to specific CQL queries. 
	 * @param currentRequest the request object representing the request
	 * @return a result set, containing the results according to the given request
	 */
	@Override
	public Result call() {
		
		if(currentRequest.getBoundStatement() != null) {
			return new ResultCassandra(currentRequest, session.execute(currentRequest.getBoundStatement()), timer.getRuntime());
		}
		
		String query = "";
		timer.reset();
		
		ResultSet tmp;
		
		// we assume we only deal with keyspace and table names in cipher form, why else would we use TimDB?
		// Table names are only given, when not creating or dropping a keyspace
		String cipherTableName = null;
		if((currentRequest.getType() != RequestType.CREATE_KEYSPACE)&&(currentRequest.getType() != RequestType.DROP_KEYSPACE)) cipherTableName = currentRequest.getId().getKeyspace().getCipherName() + "." + currentRequest.getId().getTable().getCipherName();
		
		// fill in the name of the rowkey column where necessary
		if(currentRequest.getId().getRowConditions() != null)
			for(int i=0; i<currentRequest.getId().getRowConditions().size(); i++) {				
				if(currentRequest.getId().getRowConditions().get(i).getColumnName() == null) currentRequest.getId().getRowConditions().get(i).setColumnName(currentRequest.getId().getTable().getRowkeyColumnName());
			}
		
		switch(currentRequest.getType()) {
		
		case UPDATE_SET:
			
			// compose update query
			
			String columnName = null;
			
			if(!currentRequest.getStringArgs().isEmpty()) columnName = currentRequest.getStringArgs().keySet().iterator().next();
			else if(!currentRequest.getIntArgs().isEmpty()) columnName = currentRequest.getIntArgs().keySet().iterator().next();
			else if(!currentRequest.getByteArgs().isEmpty()) columnName = currentRequest.getByteArgs().keySet().iterator().next();
			
			query = "UPDATE " + cipherTableName + " SET " + columnName + " = " + columnName + " + {";
			
			if(!currentRequest.getStringArgs().isEmpty()) query += "'" + currentRequest.getStringArgs().get(columnName) + "'";
			else if(!currentRequest.getIntArgs().isEmpty()) query += currentRequest.getStringArgs().get(columnName);
			else if(!currentRequest.getByteArgs().isEmpty()) query += /*"'" + */Misc.bytesToCQLHexString(currentRequest.getByteArgs().get(columnName)) /*+ "'"*/;
			
			query += "} WHERE " + currentRequest.getId().getRowConditions().get(0).getConditionAsString();
			
			if(currentRequest.getId().getRowConditions().size() > 1) {
				for(int i=1; i<currentRequest.getId().getRowConditions().size(); i++) {
					query += " AND " + currentRequest.getId().getRowConditions().get(i).getConditionAsString();
				}
			}
			
			query += ";";
			
			// update set
			
			timer.start();
			tmp = session.execute(query);
			timer.stop();
			
			return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
		
		case DROP_TABLE:
			
			// drop table, if exists
			query = "DROP TABLE IF EXISTS " + cipherTableName + ";";
			
			timer.start();
			tmp = session.execute(query);
			timer.stop();
			
			return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
			
		case DROP_KEYSPACE:
			
			// drop keyspace, if exists
			query = "DROP KEYSPACE IF EXISTS " + currentRequest.getId().getKeyspace().getCipherName() + ";";
			
			timer.start();
			tmp = session.execute(query);
			timer.stop();
			
			return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
			
		case CREATE_KEYSPACE:
			
			// create keyspace, if not exists
			
			// parameter map ('class': 'SimpleStrategy', 'replication_factor' : 1, etc...) aus den String/Int Arrays auslesen, falls vorhanden
			String replication_class = "SimpleStrategy";
			String replication_factor = "1";
			
			// consider non-default values for keyspace details, if given
			if(currentRequest.getStringArgs() != null) {
				if(currentRequest.getStringArgs().get("replication_class") != null) replication_class = currentRequest.getStringArgs().get("replication_class");
				if(currentRequest.getStringArgs().get("replication_factor") != null) replication_factor = currentRequest.getStringArgs().get("replication_factor");
			}
			
			timer.start();
			tmp = session.execute("CREATE KEYSPACE IF NOT EXISTS " + currentRequest.getId().getKeyspace().getCipherName() + " WITH replication = {'class': '" + replication_class + "', 'replication_factor' : " + replication_factor + " }");
			timer.stop();
			
			return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
							
		case CREATE_TABLE:
			
			// compose table creation query
			query = "CREATE TABLE " + cipherTableName + " (";
			
			for(String s : currentRequest.getStringArgs().keySet()) {
				query += s + " ";
				if(currentRequest.getStringArgs().get(s).equals("null")) query += "text,";
				else if(currentRequest.getStringArgs().get(s).equals("set")) query += "set<text>,";
				else if(currentRequest.getStringArgs().get(s).equals("list")) query += "list<text>,";
				else if(currentRequest.getStringArgs().get(s).equals("primarykey")) query += "text PRIMARY KEY,";
			}
			
			for(String s : currentRequest.getIntArgs().keySet()) {
				query += s + " ";
				if(currentRequest.getIntArgs().get(s) == 0) query += "bigint,";
				else if(currentRequest.getIntArgs().get(s) == 1) query += "set<bigint>,";
				else if(currentRequest.getIntArgs().get(s) == 2) query += "list<bigint>t,";
				else if(currentRequest.getIntArgs().get(s) == 3) query += "bigint PRIMARY KEY,";
			}
			
			for(String s : currentRequest.getByteArgs().keySet()) {
				query += s + " ";
				if(Arrays.equals(currentRequest.getByteArgs().get(s), "null".getBytes())) query += "blob,";
				else if(Arrays.equals(currentRequest.getByteArgs().get(s), "set".getBytes())) query += "set<blob>,";
				else if(Arrays.equals(currentRequest.getByteArgs().get(s), "list".getBytes())) query += "list<blob>,";
				else if(Arrays.equals(currentRequest.getByteArgs().get(s), "primarykey".getBytes())) query += "blob PRIMARY KEY,";
			}
			
			for(String s : currentRequest.getTimestampStringArgs().keySet()) {
				query += s + " ";
				if(currentRequest.getTimestampStringArgs().get(s).equals("null")) query += "timestamp,";
				else if(currentRequest.getTimestampStringArgs().get(s).equals("set")) query += "set<timestamp>,";
				else if(currentRequest.getTimestampStringArgs().get(s).equals("list")) query += "list<timestamp>,";
				else if(currentRequest.getTimestampStringArgs().get(s).equals("primarykey")) query += "timestamp PRIMARY KEY,";
			}
			
			query = query.substring(0, query.length()-1);
			query += ");";
			
			
			// create table
			//System.out.println(query);
			timer.start();
			tmp = session.execute(query);
			timer.stop();
			
			return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
		
		case INSERT: 
			
			SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			
			// compose query string for bound statement
			query = "INSERT INTO " + cipherTableName + " (";
			
			for(String s : currentRequest.getStringArgs().keySet()) query += s + ",";
			for(String s : currentRequest.getIntArgs().keySet()) query += s + ",";
			for(String s : currentRequest.getByteArgs().keySet()) query += s + ",";
			for(String s : currentRequest.getTimestampStringArgs().keySet()) query += s + ",";
			
			for(String s : currentRequest.getStringSets().keySet()) query += s + ",";
			for(String s : currentRequest.getIntSets().keySet()) query += s + ",";
			for(String s : currentRequest.getByteSets().keySet()) query += s + ",";
			for(String s : currentRequest.getTimestampStringSets().keySet()) query += s + ",";
			
			query = query.substring(0, query.length()-1);
			
			query += ") VALUES (";
			for(int i=0; i<(currentRequest.getStringArgs().keySet().size() + currentRequest.getIntArgs().keySet().size() + currentRequest.getByteArgs().keySet().size() + currentRequest.getTimestampStringArgs().keySet().size() + currentRequest.getStringSets().keySet().size() + currentRequest.getIntSets().keySet().size() + currentRequest.getByteSets().keySet().size() + currentRequest.getTimestampStringSets().keySet().size()); i++) {
				query += "?,";
			}	
			
			query = query.substring(0, query.length()-1);
			query += ");";
			
			// create bound statement
			BoundStatement insertStatement = registerStatement(query, query).boundStatementBuilder().setConsistencyLevel(DefaultConsistencyLevel.fromCode(ConsistencyLevel.ONE)).build();
			
			// insert values
			for(String s : currentRequest.getStringArgs().keySet()) insertStatement = insertStatement.setString(s, currentRequest.getStringArgs().get(s));
			for(String s : currentRequest.getIntArgs().keySet()) insertStatement = insertStatement.setLong(s, currentRequest.getIntArgs().get(s));
			for(String s : currentRequest.getByteArgs().keySet()) insertStatement = insertStatement.setByteBuffer(s, ByteBuffer.wrap(currentRequest.getByteArgs().get(s)));
			for(String s : currentRequest.getTimestampStringArgs().keySet())
				try {
					insertStatement.set(s, df.parse(currentRequest.getTimestampStringArgs().get(s)), Date.class);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			
			for(String s : currentRequest.getStringSets().keySet()) insertStatement.setSet(s, currentRequest.getStringSets().get(s), String.class);
			for(String s : currentRequest.getIntSets().keySet()) insertStatement.setSet(s, currentRequest.getIntSets().get(s), Long.class);
			for(String s : currentRequest.getByteSets().keySet()) insertStatement.setSet(s, currentRequest.getByteSets().get(s), ByteBuffer.class);
			for(String s : currentRequest.getTimestampStringSets().keySet()) insertStatement.setSet(s, currentRequest.getTimestampStringSets().get(s), String.class);
					
			
			// execute
			timer.start();
			tmp = session.execute(insertStatement);
			timer.stop();
			
			
			return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
			
		case READ:
			
			if(!cipherTableExists(currentRequest.getId())) {
				System.out.println("Can't read from: " + currentRequest.getId().getKeyspace().getCipherName() + "." + currentRequest.getId().getTable().getCipherName());
				return null;
			}
			else {
				
				// compose query
				query = "SELECT ";
			
				// always add the primary key
				query += currentRequest.getId().getTable().getRowkeyColumnName();
				
				// always add IV column
				query += ", " + currentRequest.getId().getTable().getIVcolumnName();
				
				for(int i=0; i<currentRequest.getId().getColumns().size(); i++) query += ", " + currentRequest.getId().getColumns().get(i); 
				//query = query.substring(0, query.length()-1);
			
				query += " FROM " + cipherTableName;
				
				boolean nonSEConditions = false;
				if((currentRequest.getId().getRowConditions() != null)&&(!currentRequest.getId().getRowConditions().isEmpty()))
					for(int i=0; i<currentRequest.getId().getRowConditions().size(); i++) 
						if(!currentRequest.getId().getRowConditions().get(i).getComparator().equals("#")) nonSEConditions = true;
				
				// add non SE row conditions
				if(nonSEConditions) {
					
					query += " WHERE ";
					
					if(currentRequest.getId().getRowConditions().size() == 1) query += currentRequest.getId().getRowConditions().get(0).getConditionAsString();
				
					if(currentRequest.getId().getRowConditions().size() > 1) {
						
						boolean firstCondition = true;
						
						for(int i=0; i<currentRequest.getId().getRowConditions().size(); i++) {
							if(!currentRequest.getId().getRowConditions().get(i).getComparator().equals("#")) {
								if(!firstCondition) query += " AND "; 
								query += currentRequest.getId().getRowConditions().get(i).getConditionAsString();
								firstCondition = false;
							}
						}
					}
				}
				
				if( (currentRequest.getId().getRowConditions() != null) && (!currentRequest.getId().getRowConditions().isEmpty()) ) query += " ALLOW FILTERING";
				query += ";";
				
			
				System.out.println(query);
				timer.start();
				tmp = session.execute(query);
				timer.stop();
				
				return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
			}
			
		case READ_WITHOUT_IV:
			
			if(!cipherTableExists(currentRequest.getId())) {
				System.out.println("Can't read from: " + currentRequest.getId().getKeyspace().getCipherName() + "." + currentRequest.getId().getTable().getCipherName());
				return null;
			}
			else {
				
				// compose query
				query = "SELECT ";
			
				// always add the primary key
				query += currentRequest.getId().getTable().getRowkeyColumnName();
				
				for(int i=0; i<currentRequest.getId().getColumns().size(); i++) query += ", " + currentRequest.getId().getColumns().get(i); 
				//query = query.substring(0, query.length()-1);
			
				query += " FROM " + cipherTableName;
				
				boolean nonSEConditions = false;
				if((currentRequest.getId().getRowConditions() != null)&&(!currentRequest.getId().getRowConditions().isEmpty()))
					for(int i=0; i<currentRequest.getId().getRowConditions().size(); i++) 
						if(!currentRequest.getId().getRowConditions().get(i).getComparator().equals("#")) nonSEConditions = true;
				
				// add non SE row conditions
				if(nonSEConditions) {
					
					query += " WHERE ";
					
					if(currentRequest.getId().getRowConditions().size() == 1) query += currentRequest.getId().getRowConditions().get(0).getConditionAsString();
				
					if(currentRequest.getId().getRowConditions().size() > 1) {
						
						boolean firstCondition = true;
						
						for(int i=0; i<currentRequest.getId().getRowConditions().size(); i++) {
							if(!currentRequest.getId().getRowConditions().get(i).getComparator().equals("#")) {
								if(!firstCondition) query += " AND "; 
								query += currentRequest.getId().getRowConditions().get(i).getConditionAsString();
								firstCondition = false;
							}
						}
					}
				}
				
				if( (currentRequest.getId().getRowConditions() != null) && (!currentRequest.getId().getRowConditions().isEmpty()) ) query += " ALLOW FILTERING";
				query += ";";
							
				//System.out.println(query);
				timer.start();
				tmp = session.execute(query);
				timer.stop();
				
				return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
			}
			
		case READ_WITH_SET_CONDITION:
			
			if(!cipherTableExists(currentRequest.getId())) {
				System.out.println("Can't read from: " + currentRequest.getId().getKeyspace().getCipherName() + "." + currentRequest.getId().getTable().getCipherName());
				return null;
			}
			else {
				
				// compose query
				query = "SELECT ";
			
				// always add the primary key
				query += currentRequest.getId().getTable().getRowkeyColumnName();
				
				for(int i=0; i<currentRequest.getId().getColumns().size(); i++) query += ", " + currentRequest.getId().getColumns().get(i); 
				//query = query.substring(0, query.length()-1);
			
				query += " FROM " + cipherTableName;
				
				// add non SE row conditions
				if((!currentRequest.getId().getRowConditions().isEmpty())&&(!currentRequest.getId().getRowConditions().get(0).getComparator().equals("#"))) {
				
					query += " WHERE " + currentRequest.getId().getRowConditions().get(0).getColumnName() + " IN (";
					
					if(currentRequest.getId().getRowConditions().get(0).getType() == ColumnType.STRING) query += "'" + currentRequest.getId().getRowConditions().get(0).getStringTerm() + "'";
					if(currentRequest.getId().getRowConditions().get(0).getType() == ColumnType.INTEGER) query += String.valueOf(currentRequest.getId().getRowConditions().get(0).getLongTerm());
					if(currentRequest.getId().getRowConditions().get(0).getType() == ColumnType.BYTE) query += "'" + currentRequest.getId().getRowConditions().get(0).getByteTerm() + "'";
					
					if(currentRequest.getId().getRowConditions().size() > 1) {
						for(int i=1; i<currentRequest.getId().getRowConditions().size(); i++) {
							if((currentRequest.getId().getRowConditions().get(i).getComparator().equals("=")) && (currentRequest.getId().getRowConditions().get(0).getColumnName().equals(currentRequest.getId().getRowConditions().get(i).getColumnName()))) {
								
								if(currentRequest.getId().getRowConditions().get(i).getType() == ColumnType.STRING) query += ", '" + currentRequest.getId().getRowConditions().get(i).getStringTerm() + "'";
								if(currentRequest.getId().getRowConditions().get(i).getType() == ColumnType.INTEGER) query += ", " + String.valueOf(currentRequest.getId().getRowConditions().get(i).getLongTerm());
								if(currentRequest.getId().getRowConditions().get(i).getType() == ColumnType.BYTE) query += ", '" + currentRequest.getId().getRowConditions().get(i).getByteTerm() + "'";
							}
						}
					}
				}
				
				if( (currentRequest.getId().getRowConditions() != null) && (!currentRequest.getId().getRowConditions().isEmpty()) ) query += " ALLOW FILTERING";
				query += ";";
							
				//System.out.println(query);
				timer.start();
				tmp = session.execute(query);
				timer.stop();
				
				return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
			}
			
		case UPDATE_VALUE:
			
			// compose update query
			query = "UPDATE " + currentRequest.getId().getKeyspace() + "." + currentRequest.getId().getTable() + " SET ";// + r.getId().getColumns()[0] + " = ";
					
			String tmp_key = null;
			
			if(!currentRequest.getStringArgs().isEmpty()) {
				tmp_key = currentRequest.getStringArgs().keySet().iterator().next();
				query += tmp_key + " = '" + currentRequest.getStringArgs().get(tmp_key) + "'";
			}
			else if(!currentRequest.getIntArgs().isEmpty()) {
				tmp_key = currentRequest.getStringArgs().keySet().iterator().next();
				query += tmp_key + " = " + currentRequest.getIntArgs().get(tmp_key);
			}
			else if(!currentRequest.getByteArgs().isEmpty()) {
				tmp_key = currentRequest.getStringArgs().keySet().iterator().next();
				query += tmp_key + " = '" + currentRequest.getByteArgs().get(tmp_key) + "'";
			}
			else if(!currentRequest.getTimestampStringArgs().isEmpty()) {
				tmp_key = currentRequest.getStringArgs().keySet().iterator().next();
				query += tmp_key + " = '" + currentRequest.getTimestampStringArgs().get(tmp_key) + "'";
			}
			
			if(!currentRequest.getId().getRowConditions().isEmpty()) query += " WHERE " + currentRequest.getId().getRowConditions().get(0).getConditionAsString();
			
			if(currentRequest.getId().getRowConditions().size() > 1) {
				for(int i=1; i<currentRequest.getId().getRowConditions().size(); i++) {
					
					query += " AND " + currentRequest.getId().getRowConditions().get(i).getConditionAsString();
				}
			}
			
			query += ";";	
						
			// update
			timer.start();
			tmp = session.execute(query);
			timer.stop();
			
			return new ResultCassandra(currentRequest, tmp, timer.getRuntime());
			
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
		
		// column key besorgen
		byte[] key = cs.getKey();
		
		// rowkeys, IVs und column Inhalt besorgen
		TableState table = cs.getTable();
		
		String columnName = null;
		
		String rowkeyColumnName = null;
		if(table.getRowkeyColumn().isEncrypted()) rowkeyColumnName = table.getRowkeyColumn().getCOPEname();
		else rowkeyColumnName = table.getRowkeyColumn().getPlainName();
		
		if(onion.equals("DET")) columnName = cs.getCDETname();
		if(onion.equals("OPE")) columnName = cs.getCOPEname();
		
		String query = "SELECT " + rowkeyColumnName + ", " + table.getIVcolumnName() + ", " + columnName + " ";
		query += "FROM " + table.getKeyspace().getCipherName() + "." + table.getCipherName() + ";";
		
		// TODO: this has to be rewritten for larger tables, no time for that
		
		ResultSet result = session.execute(query);
		
		if(result.iterator().hasNext()){
			
			Iterator<Row> it = result.iterator();
			PreparedStatement updateQuery = registerStatement("upd000",
					"UPDATE " + table.getKeyspace().getCipherName() + "." + table.getCipherName() + " " + 
				    "SET " + columnName + "=? " +
					"WHERE " + rowkeyColumnName + "=?;");
			
			// für alle rows
			while(it.hasNext()) {
				Row row = it.next();
				
				byte[] encryptedValue = new byte[row.getByteBuffer(columnName).remaining()];
				row.getByteBuffer(columnName).get(encryptedValue);
				
				byte[] iv = new byte[row.getByteBuffer(table.getIVcolumnName()).remaining()];
				row.getByteBuffer(table.getIVcolumnName()).get(iv);
				
				byte[] decryptedValue = cs.getRNDScheme().decrypt(encryptedValue, iv);
				
				//System.out.println("Row updated with: " + Misc.bytesToLong(decryptedValue));
				
				BoundStatement updateQueryStatement = updateQuery.bind(ByteBuffer.wrap(decryptedValue), row.getObject(rowkeyColumnName));
				session.execute(updateQueryStatement);		
			}
			
		}
				
		// flag setzen
		if(onion.equals("DET")) cs.setRNDoverDETStrippedOff(true);
		if(onion.equals("OPE")) cs.setRNDoverOPEStrippedOff(true);
		
		t.stop();
		//System.out.println("RND layer removed from column \"" + cs.getPlainName() + "\" (" + t.getRuntimeAsString() + ")");
	}



	@Override
	public PreparedStatement registerStatement(String label, String query) {
		
		if(!preparedStatements.containsKey(label)) preparedStatements.put(label, session.prepare(query));
		
		return preparedStatements.get(label);
		
	}

}
