package databases;

import java.util.ArrayList;
import java.util.List;

import misc.Misc;

import org.jdom2.Element;

import crypto.KeyStoreManager;

import crypto.PRG;
import databases.DBClientFactory;
import enums.DatabaseType;
import interfaces.SaveableInXMLElement;



/**
 * Class for managing the metadata of a keyspace/namespace
 * 
 * @author Tim Waage
 */
public class KeyspaceState implements SaveableInXMLElement {

	// available DBClients that can store the columns of this table
	private ArrayList<DBClient> dbs = new ArrayList<DBClient>(); 
	
	// the plaintext name of the keyspace
	private String pname;
	
	// the ciphertext name of the keyspace
	private String cname;
	
	// the list of tables present in this keyspace
	private ArrayList<TableState> tables = new ArrayList<TableState>();
	
	// the keystore the encryption schemes in this keyspace are using
	private KeyStoreManager keystore;
	
	
	
	
	
	/**
	 * Constructor
	 * @param _pname the plaintext name of the keyspace
	 * @param _dbs a list of the available database connections
	 * @param the keystore this keyspace will be using to manage the keys for the encryption of its tables
	 */
	public KeyspaceState(String _pname, ArrayList<DBClient> _dbs, KeyStoreManager _keystore) {
		
		PRG g = new PRG();
		
		pname = _pname;
		dbs = _dbs;
		
		// generate an "encrypted" keyspace name
		cname = "enc_" + pname;//Misc.ByteArrayToCharString(g.generateRandomTableOrColumnName(8));
		
		keystore = _keystore;
		
	}
	
	
	
	/**
	 * Constructor
	 * @param data keyspace metadata in XML form
	 * @param the keystore this keyspace will be using to manage the keys for the encryption of its tables
	 */
	public KeyspaceState(Element data, KeyStoreManager _keystore) {
		
		keystore = _keystore;
		this.initializeFromXMLElement(data);
	}
	
	
	
	/**
	 * Returns a list of the available database connections
	 * @return the available database connections
	 */
	public ArrayList<DBClient> getAvailableDatabases() {
		
		return dbs;
	}
	
	
	
	/**
	 * Returns all tables associated to this keyspace
	 * @return all tables associated to this keyspace
	 */
	public ArrayList<TableState> getAvailableTables() {
		
		return tables;
	}
	
	
	
	/**
	 * Returns the plaintext name of this keyspace
	 * @return the plaintext name of this keyspace
	 */
	public String getPlainName() {
		
		return pname;
	}
	
	
	
	/**
	 * Returns the ciphertext name of this keyspace
	 * @return the ciphertext name of this keyspace
	 */
	public String getCipherName() {
		
		return cname;
	}
	
	
	/**
	 * Returns the keystore the schemes in this keyspace are using
	 * @return the keystore the schemes in this keyspace are using
	 */
	public KeyStoreManager getKeystore() {
		
		return keystore;
	}
	
	
	/**
	 * add a new table to the keyspace
	 * @param ts the TableState of the new Table
	 */
	public void addTableState(TableState ts) {
		
		tables.add(ts);
	}
	
	
	
	/**
	 * returns all TableState objects representing the same logical table, given by the name in the parameter
	 * @param tablename the name of the desired table
	 * @return all TableState objects representing the same logical table, given by the name in the parameter
	 */
	public TableState[] getTableByPlainName(String tablename) {
		
		int numberOfTableStates = 0;
		
		for(TableState ts : tables) {
			if(ts.getPlainName().equals(tablename)) numberOfTableStates++;
		}
		
		if(numberOfTableStates == 0) return null;
		else{
		
		TableState[] table = new TableState[numberOfTableStates];
		
			int i = 0;
			for(TableState ts : tables) {
				if(ts.getPlainName().equals(tablename)) {
					table[i] = ts;
					i++;
				}		                                                  
			}		
			return table;
		}
		
	}
	
	
	
	/**
	 * For manually setting the cipher name, not nice, but needed for the SUISE index
	 * @param _ciperName
	 */
	public void setCipherName(String _cipherName) {
		
		cname = _cipherName;
	}
	
	
	
	@Override
	public Element getThisAsXMLElement() {
		
		Element keyspaceRoot = new Element("keyspace");
		
		//plaintext name
		Element keyspacePlainName = new Element("plainname");
		keyspacePlainName.addContent(pname);
		keyspaceRoot.addContent(keyspacePlainName);
		
		//ciphertext name
		Element keyspaceCipherName = new Element("ciphername");
		keyspaceCipherName.addContent(cname);
		keyspaceRoot.addContent(keyspaceCipherName);
		
		//tables
		Element keyspaceTables = new Element("tables");
		for(TableState ts : tables) keyspaceTables.addContent(ts.getThisAsXMLElement());
		keyspaceRoot.addContent(keyspaceTables);
		
		//database connections
		Element keyspaceDatabases = new Element("databases");
		for(DBClient db : dbs) keyspaceDatabases.addContent(db.getThisAsXMLElement());
		keyspaceRoot.addContent(keyspaceDatabases);
		
		return keyspaceRoot;
	}

	
	
	@Override
	public void initializeFromXMLElement(Element data) {
		
		// plain-/ciphername
		this.pname = data.getChild("plainname").getText();
		this.cname = data.getChild("ciphername").getText();
		
		// tables
		List<Element> tbls = data.getChild("tables").getChildren("table");
		for(Element tbl : tbls) tables.add(new TableState(tbl, this));
		
		// initialize database connections
		List<Element> dbsElements = data.getChild("databases").getChildren("db");
		for(Element db : dbsElements) dbs.add(DBClientFactory.getDBClient(DatabaseType.valueOf(db.getChildText("type")), db.getChildText("ip")));
	}

}
