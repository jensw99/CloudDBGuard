package databases;

import java.util.ArrayList;

import enums.DatabaseType;

/**
 * This class keeps track of all database connections
 * @author Tim Waage
 *
 */
public class DBClientFactory {
		
	// an ArrayList of all instantiated DBClients
	private static ArrayList<DBClient> clients = new ArrayList<DBClient>();
	
	
	
	/**
	 * Method for acquiring new DBClients 
	 * @param _type the desired database type
	 * @param _ip the IP address of the targeted database
	 * @return an appropriate DBClient object
	 */
	public static DBClient getDBClient(DatabaseType _type, String _ip) {
		
		// look if an appropriate DBClient was already instantiated
		for(DBClient db : clients) {
			if((db.getType() == _type)&&(db.getIP().equals(_ip))) return db;
		}
		
		// ...if not: do it now
		DBClient newClient = null;
		
		System.out.println("getDBClient _ip="+_ip);
		
		if(_type == DatabaseType.CASSANDRA) newClient = new DBClientCassandra(_ip);
		else if(_type == DatabaseType.HBASE) newClient = new DBClientHBase(_ip);
		
		clients.add(newClient);
		
		return newClient;
	}
	
	
	
	/**
	 * closes all open database connections
	 */
	public static void closeAllConnections() {
		
		for(DBClient db : clients) db.close();
		clients.removeAll(clients);
	}

}
