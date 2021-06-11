import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.protocol.internal.ProtocolConstants.ConsistencyLevel;

import misc.Misc;
import misc.Timer;

public class DBCassandraUnencrypted {
	
	private CqlSession session;
	private PreparedStatement ps;
	private Timer timer;
	
	
	public DBCassandraUnencrypted(String _ip, int _port) {
		session = CqlSession.builder()
			    .addContactPoint(new InetSocketAddress(_ip, _port))
			    .withLocalDatacenter("datacenter1")
			    .build();
		timer = new Timer();
	}
	
	public void createEnronKeyspace() {
		session.execute("create keyspace enron_unencrypted WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}");

	}
	
	public void dropEnronKeyspace() {
		session.execute("DROP KEYSPACE IF EXISTS enron_unencrypted");
	}
	
	public void createEnronTable() {
		String query = "CREATE TABLE enron_unencrypted.enron ("
				+ "id TEXT PRIMARY KEY,"
				+ "sender TEXT,"
				+ "receiver set<TEXT>,"
				+ "cc set<TEXT>,"
				+ "bcc set<TEXT>,"
				+ "subject TEXT,"
				+ "body TEXT,"
				+ "path TEXT,"
				+ "year INT,"
				+ "month INT,"
				+ "day INT,"
				+ "size INT,"
				+ "timestamp INT,"
				+ "xcc TEXT,"
				+ "xfolder TEXT,"
				+ "xorigin TEXT,"
				+ "mimeversion TEXT,"
				+ "xbcc TEXT,"
				+ "xfilename TEXT,"
				+ "xto TEXT,"
				+ "cte TEXT,"
				+ "xfrom TEXT,"
				+ "xte TEXT,"
				+ "contenttype TEXT,"
				+ "writer TEXT,"
				+ ")";;
		session.execute(query);
;
	}
	
	public long insertRow(HashMap <String, String> rowStrings, HashMap<String, Long> rowLongs, HashMap<String, HashSet<String>> rowStringSets) {
		timer.reset();
		if (ps == null) {
			String query = "INSERT INTO enron_unencrypted.enron(";
			
			String queryValues = " VALUES(";
			
			for (String column : rowStrings.keySet()) {
		          query += column + ",";
		          queryValues += "?,";
		    }
			for (String column : rowLongs.keySet()) {
		          query += column + ",";
		          queryValues += "?,";
		    }
			for (String column : rowStringSets.keySet()) {
		          query += column + ",";
		          queryValues += "?,";
		    }
			
			// Remove last ,
			query = query.substring(0, query.length()-1);
			queryValues = queryValues.substring(0, queryValues.length()-1);
			
			query = query + ")" + queryValues + ")";
			
			System.out.println(query);
			
			ps = session.prepare(query);
		}
		// BoundStatement bs = ps.bind();
		BoundStatement bs = ps.boundStatementBuilder().setConsistencyLevel(DefaultConsistencyLevel.fromCode(ConsistencyLevel.ONE)).build();
		
		for (Map.Entry<String, String> e : rowStrings.entrySet()) bs = bs.setString(e.getKey(), e.getValue());
		for (Map.Entry<String, Long> e : rowLongs.entrySet()) bs = bs.setInt(e.getKey(), e.getValue().intValue());
		for (Map.Entry<String, HashSet<String>> e : rowStringSets.entrySet()) bs = bs.setSet(e.getKey(), e.getValue(), String.class);
		
		timer.start();
		session.execute(bs);
		timer.stop();
		
		return timer.getRuntime();
		
		
	}
	
	public long query(String[] columns, String keyspace, String table, String[] conditions) {
		timer.reset();
		String query = "";
		
		HashMap<String, String> search = new HashMap<String, String>();
		
		boolean first = true;
		
		for (String cond : conditions) {
			String operator = "";
			boolean textCol = false;
			
			if (cond.split("=").length > 1) operator = "=";
			if (cond.split("#").length > 1) operator = "#";
			if (cond.split("<").length > 1) operator = "<";
			if (cond.split(">").length > 1) operator = ">";
			
			String column = cond.split(operator)[0];
			String value = cond.split(operator)[1];
			
			
			if (operator.equals("#")) {
				search.put(column, value);
				continue;
			}
			
			if (column.equalsIgnoreCase("receiver")) {
				operator = " CONTAINS ";
			}
			
			if(column.equals("receiver") || column.equals("sender") || column.equals("body") || column.equals("subject") || column.equals("writer")) textCol = true;
			
			if (first) query += " WHERE ";
			
			if (!first) query += " AND ";
			
			if (value.split(",").length > 1) {
				String[] values = value.split(",");
				
				if(textCol) {
					query += column + operator +"'" + value.split(",")[0].trim() +"'";
				}else{
					query += column + operator + value.split(",")[0].trim();
				}
				
				for (int i = 1; i < values.length; i++) {
					if(textCol) {
						query +=" AND "+ column + operator +"'" + value.split(",")[i].trim() +"'";
					}else{
						query +=" AND "+ column + operator + value.split(",")[i].trim();
					}
					
				}
				
				first = false;
			}else {
				if (textCol) {
					query += column+operator+"'"+value.trim()+"'";
				}else {
					query += column+operator+value.trim();
				}
				first = false;
			}
		}
		if (!first) query+= " ALLOW FILTERING";
		
		String querySelect = "SELECT ";
		for (String col : columns) querySelect += col + ",";
		for (String col : search.keySet()) querySelect += col + ",";
		// Remove last ,
		querySelect = querySelect.substring(0, querySelect.length()-1);
		
		querySelect += " FROM " + keyspace + "." + table;
		
		query = querySelect + query;
		
		System.out.println(query);
		
		SimpleStatement sstmt = SimpleStatement.newInstance(query).setTimeout(Duration.ofSeconds(5));
		
		timer.start();
		ResultSet rs = session.execute(sstmt);
		// for (ColumnDefinition cd : rs.getColumnDefinitions()) System.out.println(cd.getName().toString());
		
		if(search.size() > 0) {
			int counter = 0;
			for(Row r : rs) {
				boolean in = true;
				for (Map.Entry<String, String> e : search.entrySet()) {
					if (!lookup(r.getString(e.getKey()), e.getValue())) {
						in = false;
					}
				}
				if (in) counter++;
			}
			System.out.println(counter);
			ClientQueryUnencrypted.counter += counter;
		}else {
			System.out.println(rs.all().size());
			ClientQueryUnencrypted.counter += rs.all().size();
		}
		timer.stop();
		
		
		// System.out.println(timer.getRuntimeAsString());
		
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
		session.close();
	}
}
