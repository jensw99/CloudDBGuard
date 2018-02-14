package databases;

import interfaces.SaveableInXMLElement;

import java.util.ArrayList;
import java.util.List;

import org.jdom2.Element;

import misc.Misc;
import crypto.PRG;
import enums.TableProfile;
import enums.DatabaseType;



/**
 * Class for storing information about the encryption of a table
 * @author Tim Waage
 *
 */
public class TableState implements SaveableInXMLElement{
	
	private byte[] iv; //IV for this tables DET encryption
	private byte[] key; // key for this tables DET encryption
		
	private String pName; // this tables name in plaintext
	private String cName; // this tables name in ciphertext 
	
	private ArrayList<ColumnState> columnStates = new ArrayList<ColumnState>(); // this tables columns
	private TableProfile profile; // specifies the encryption schemes used for this table
	
	private KeyspaceState keyspace; // a reference back to the keyspace this table belongs to (for getting the available DBs)
	
	private DBClient db; // the database this table is stored on
	
	private String rowkeyColumnName; // ciphertext name of the column representing the rowkey
	
	private String IVcolumnName; // name of the column containing the IVs
	
	
	
	/**
	 * Constructor, used only when tables are created
	 * 
	 * @param _plainId the Tables ID in Plaintext
	 * @param _dbs the DBClients available/used for this table
	 * @param _key key for this tables DET encryption
	 */
	public TableState(String _pName, KeyspaceState _keyspace, DBClient _db, TableProfile _profile, byte[] _iv, byte[] _key) {
		
		keyspace = _keyspace;
		
		PRG g = new PRG();
		
		iv = _iv;	// AES IV	
		key = _key;	// AES key for DET encryption
		
		// TODO: put theses keys into the keystore
		
		pName = _pName;
		
		cName = "enc_" + _pName;//Misc.ByteArrayToCharString(g.generateRandomTableOrColumnName(8)); // "encrypted" tablename
						 
		profile = _profile;
		
		db = _db;
		
		IVcolumnName = Misc.ByteArrayToCharString(g.generateRandomTableOrColumnName(32));
	}
	
	
	
	/**
	 * Constructor
	 * @param data LinkedHashMap containing all information to initialize the table state
	 */
	public TableState(Element data, KeyspaceState _keyspace) {
	
		keyspace = _keyspace;
		this.initializeFromXMLElement(data);	
	}
	
	
	
	/**
	 * Returns this tables column profile
	 * @return this tables ColumnProfile object
	 */
	public TableProfile getProfile() {
		
		return profile;
	}
	
	
	
	/**
	 * returns the keyspace this table belongs to
	 * @return the KeyspaceState object representing the keyspace this table belongs to
	 */
	public KeyspaceState getKeyspace() {
		
		return keyspace;
	}
	
	
	
	/**
	 * Returns the database connection this tables is stored to
	 * @return the database connection this tables is stored to
	 */
	public DBClient getDBClient() {
		
		return db;
	}
	
	
	
	/**
	 * returns this tables IV for deterministic AES encryption
	 * @return this tables IV
	 */
	public byte[] getIV() {
		
		return iv;
	}
	
	
	
	/**
	 * returns the name of the column containing the IVs for RND encryption
	 * @return the name of the column containing the IVs for RND encryption
	 */
	public String getIVcolumnName() {
		
		return IVcolumnName;
	}
	
	
	
	/**
	 * returns this tables table key for deterministic AES encryption
	 * @return this tables table key
	 */
	public byte[] getKey() {
		
		return key;
	}
	
		
	
	/**
	 * Returns the plain name of the table
	 * @return the plain name of the table
	 */
	public String getPlainName() {
		
		return pName;
	}
	
	
	
	/**
	 * Returns the cipher name of the table
	 * @return the cipher name of the table
	 */
	public String getCipherName() {
		
		return cName;
	}
	
	
	
	/**
	 * Adds a column to the table
	 * @param cs the ColumnState of the new column
	 */
	public void addColumnState(ColumnState cs) {
		
		// add column state to list
		columnStates.add(cs);
		
		// if the new column is the rowkey column
		if(cs.isRowkeyColumn()) {
			// if encrypted, set it's OPE part the table's rowkey
			if(cs.isEncrypted()) this.rowkeyColumnName = cs.getCOPEname();
			// if unencrypted, set it's plain name the tables rowkey
			else this.rowkeyColumnName = cs.getPlainName();
		}	
			
		cs.adjustColumnToTable(this);
		
	}
	
	
	
	/**
	 * Returns a list of columns belonging to this table
	 * @return a list of columns belonging to this table
	 */
	public ArrayList<ColumnState> getAvailableColumns() {
		
		return columnStates;
	}
	
	
	
	/**
	 * returns the columnState of the column with the given plantext name, null if no such column exists
	 * @param name the name of the desired column
	 * @return the columnState of the column with the given plantext name, null if no such column exists
	 */
	public ColumnState getColumnByPlainName(String name) {
		
		for(ColumnState cs: columnStates) {
			if(cs.getPlainName().equals(name)) return cs;
		}
		
		return null;
	}
	
	
	
	/**
	 * returns the column where one of the ciphertext column names matches the given string
	 * @param name of a ciphertext column
	 * @return the column where one of the ciphertext column names matches the given string
	 */
	public ColumnState getColumnByCipherName(String name) {
		
		for(ColumnState cs: columnStates) {
			if(cs.getCRNDname().equals(name)) return cs;
			if(cs.getCDETname().equals(name)) return cs;
			if(cs.getCOPEname().equals(name)) return cs;
			if(cs.getCSEname().equals(name)) return cs;
		}
		
		return null;
		
	}
	
	
	
	/**
	 * returns the columnState of the column with the given plain or cipher name, null if no such column exists
	 * @param name the name of the desired column
	 * @return the columnState of the column with the given plain or cipher name, null if no such column exists
	 */
	public ColumnState getColumnByName(String name) {
		
		for(ColumnState cs: columnStates) {
			if(cs.getPlainName().equals(name)) return cs;
			if(cs.getCRNDname().equals(name)) return cs;
			if(cs.getCDETname().equals(name)) return cs;
			if(cs.getCOPEname().equals(name)) return cs;
			if(cs.getCSEname().equals(name)) return cs;
		}
		
		return null;
	}

	
	
	/**
	 * returns the rowkey column of a table
	 * @return the rowkey column of a table
	 */
	public ColumnState getRowkeyColumn() {
		
		for(ColumnState cs : columnStates)
			if(cs.isRowkeyColumn()) return cs;
		
		// should not happen
		return null;
	}
	
	
	
	/**
	 * returns the plaintext name of the column containing the rowkeys
	 * @return the plaintext name of the column containing the rowkeys
	 */
	public String getPlainRowkeyColumnName() {
		
		for(ColumnState cs : columnStates) {			
			if(cs.isRowkeyColumn()) {				
				return cs.getPlainName();				
			}			
		}
		
		return null;
	}
	
	
	
	/**
	 * returns the name of the rowkey column as it appears in the database
	 * @return the name of the rowkey column as it appears in the database
	 */
	public String getRowkeyColumnName() {
		
		ColumnState rowkeyColumn = this.getRowkeyColumn();
		
		if(rowkeyColumn.isEncrypted()) return rowkeyColumn.getCOPEname();
		else return rowkeyColumn.getPlainName();
		
	}
	

	/**
	 * For saving the TableState back into an XML Element
	 * @return the XMLElement object representing the Table
	 */
	@Override
	public Element getThisAsXMLElement() {
		
		Element tableRoot = new Element("table");
		
		// iv
		Element tableIv = new Element("iv");
		tableIv.addContent(Misc.ByteArrayToCharString(iv));
		tableRoot.addContent(tableIv);
		
		// key
		Element tableKey = new Element("key");
		tableKey.addContent(Misc.ByteArrayToCharString(key));
		tableRoot.addContent(tableKey);
		
		// plain name
		Element plainName = new Element("plainname");
		plainName.addContent(pName);
		tableRoot.addContent(plainName);
		
		// plain name
		Element cipherName = new Element("ciphername");
		cipherName.addContent(cName);
		tableRoot.addContent(cipherName);
		
		// columns
		Element tableColumns = new Element("columns");
		for(ColumnState cs : columnStates) tableColumns.addContent(cs.getThisAsXMLElement());
		tableRoot.addContent(tableColumns);
		
		// column profile
		Element tableProfile = new Element("profile");
		tableProfile.addContent(profile.toString());
		tableRoot.addContent(tableProfile);
		
		// name of the rowkey column
		Element tableRowkeyColumn = new Element("rowkeyColumn");
		tableRowkeyColumn.addContent(rowkeyColumnName);
		tableRoot.addContent(tableRowkeyColumn);
		
		// name of the IV column
		Element tableIVColumn = new Element("IVColumn");
		tableIVColumn.addContent(IVcolumnName);
		tableRoot.addContent(tableIVColumn);
		
		// the database this table is stored in 
		tableRoot.addContent(db.getThisAsXMLElement());
			
		return tableRoot;
	}



	@Override
	public void initializeFromXMLElement(Element data) {
		
		iv = Misc.CharStringToByteArray(data.getChild("iv").getText());
		key = Misc.CharStringToByteArray(data.getChild("key").getText());
		
		pName = data.getChild("plainname").getText();
		cName = data.getChild("ciphername").getText();
		
		rowkeyColumnName = data.getChild("rowkeyColumn").getText();
		
		IVcolumnName = data.getChild("IVColumn").getText();
		
		profile = TableProfile.valueOf(data.getChild("profile").getText());
		
		db = DBClientFactory.getDBClient(DatabaseType.valueOf(data.getChild("db").getChild("type").getText()), data.getChild("db").getChild("ip").getText());
		
		List<Element> clmns = data.getChild("columns").getChildren("column");
		for(Element clmn : clmns) addColumnState(new ColumnState(clmn));
	}
	
	
	
	/**
	 * For manually setting the cipher name, not nice, but needed for the SUISE index
	 * @param _ciperName
	 */
	public void setCipherName(String _cipherName) {
		
		cName = _cipherName;
	}
	
}


