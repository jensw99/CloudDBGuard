package databases;

import java.util.concurrent.Callable;

import misc.Timer;

import org.jdom2.Element;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import interfaces.SaveableInXMLElement;
import enums.DatabaseType;
import enums.RequestType;


/**
 * Abstract class representing a connection to a database. Has to be implemented for concrete databases.
 * 
 * @author Tim Waage
 *
 */
public abstract class DBClient implements SaveableInXMLElement, Callable<Result> {

	// identifies the DB connection (e.g. "Cassandra")
	protected DatabaseType type;
	
	// the IP address to connect to
	protected String ip;
	
	// the current Request
	protected Request currentRequest;
	
	// Timer vor measuring the runtime of queries
	protected Timer timer = new Timer();
	
	
	
	/**
	 * gets the database identifier
	 * @return
	 */
	public String toString() {
		
		String result = String.valueOf(type) + " (" + ip + ")";
		
		return result;
	}
	
	
	
	/**
	 * Returns the IP this DBClient is supposed to connect to
	 * @return the IP this DBClient is supposed to connect to
	 */
	public String getIP() {
		return ip;
	}
	
	
	
	/**
	 * Returns the database this client is supposed to connect to
	 * @return the database this client is supposed to connect to
	 */
	public DatabaseType getType() {
		return type;
	}
	
	

	public DBClient(DatabaseType _type, String _ip) {
		
		type = _type;
		ip = _ip;
		
		connect();
	}

	
	
	/**
	 * Connects to a database node/cluster
	 * @param address address of the database node/cluster (usually an IP address)
	 */
	protected abstract void connect();
	
	
	
	/**
	 * Closes all objects within the database client, that have been used (if necessary)
	 */
	public abstract void close();
	
	
	
	/**
	 * Checks if a certain table exists
	 * @param id id that describes the location of the table 
	 * @return true, if the table exists, false otherwise
	 */
	public abstract boolean cipherTableExists(DBLocation id);
	
	
	
	/**
	 * Checks if a certain keyspace exists
	 * @param id id that describes the location of the keyspace 
	 * @return true, if the keyspace exists, false otherwise
	 */
	public abstract boolean cipherKeyspaceExists(DBLocation id);
	
	
	
	/**
	 * Performs interactions (requests) with the DB. Has to be implemented for every DB individually
	 * @param r the request object representing the request
	 * @return a result set, containing the results according to the given request
	 * @throws Exception 
	 */
	public Result processRequest(Request r) {
		
		setCurrentRequest(r);		
		try {
			return call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}		
	}
	
	
	
	/**
	 * Returns an XML representation of this DBClient
	 */
	public Element getThisAsXMLElement() {
		
		Element dbRoot = new Element("db");
		
		Element dbType = new Element("type");
		dbType.addContent(String.valueOf(type));
		dbRoot.addContent(dbType);
		
		Element dbIp = new Element("ip");
		dbIp.addContent(ip);
		dbRoot.addContent(dbIp);
		
		return dbRoot;
	}
	
	
	/**
	 * Removes the RND layer from an encrypted onion column
	 * @param cs the column
	 * @param onion "DET" for removing the RND layer above the DET layer, "OPE" analogous
	 */
	public abstract void removeRNDLayer(ColumnState cs, String onion);
	
	
	
	/**
	 * inserts a new row into the database, creates the appropriate table, if not exists
	 * @param insertRequest request containing the new row
	 * @param createRequest request for creating the appropriate table, if not exists
	 */
	public void insertRow(Request insertRequest, Request createRequest) {
		
		// create keyspace if not exists
		if(!cipherKeyspaceExists(insertRequest.getId())) processRequest(new Request(RequestType.CREATE_KEYSPACE, createRequest.getId()));
		
		// if table does not exist, create a table according to the given createRequest
		if(!cipherTableExists(insertRequest.getId())) processRequest(createRequest);
		
		// insert
		processRequest(insertRequest);
	}
	
	
	
	public void setCurrentRequest(Request _r) {
		
		currentRequest = _r;
	}
	
	
	public abstract PreparedStatement registerStatement(String label, String query);
	
}
