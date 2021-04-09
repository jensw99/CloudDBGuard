import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

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
	
	public long createEnronKeyspace() {
		timer.start();
		session.execute("create keyspace enron_unencrypted WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}");
		timer.stop();
		
		return timer.getRuntime();
	}
	
	public long dropEnronKeyspace() {
		timer.start();
		session.execute("DROP KEYSPACE enron_unencrypted");
		timer.stop();
		
		return timer.getRuntime();
	}
	
	public long createEnronTable() {
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
				+ ")";
		timer.start();
		session.execute(query);
		timer.stop();
		
		return timer.getRuntime();
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
		BoundStatement bs = ps.bind();
		
		for (Map.Entry<String, String> e : rowStrings.entrySet()) bs = bs.setString(e.getKey(), e.getValue());
		for (Map.Entry<String, Long> e : rowLongs.entrySet()) bs = bs.setInt(e.getKey(), e.getValue().intValue());
		for (Map.Entry<String, HashSet<String>> e : rowStringSets.entrySet()) bs = bs.setSet(e.getKey(), e.getValue(), String.class);
		
		timer.start();
		session.execute(bs);
		timer.stop();
		
		return timer.getRuntime();
		
		
	}
	
	public void close() {
		session.close();
	}
}
