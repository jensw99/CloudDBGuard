package crypto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;

import databases.DBLocation;
import enums.ColumnType;
import enums.DatabaseType;
import databases.ColumnState;
import databases.DBClient;
import databases.DBClientCassandra;
import databases.KeyspaceState;
import databases.Request;
import databases.TableState;
import enums.RequestType;
import databases.Result;
import databases.RowCondition;



/**
 * Implementation for the Index of the SUISE scheme
 * 
 * @author Tim Waage
 */
public class SE_SUISE_Index extends Index {

	// the name of the keyspace where the index is stored
	private String suiseIndexKeyspace = "suise_index";
	
	// the state of the keyspace where the index is stored
	private KeyspaceState ks;
	
	// the state of the index tables
	private TableState ts_gamma_f = null;
	private TableState ts_gamma_w = null;
	
	// a request for creating a table for the gamma_f index
	private Request gamma_f_createRequest = null;
	
	// the location inside the database that this index represents
	private DBLocation location = null;
	
	// tells if there already tables for the indexes
	private boolean gamma_w_table_exists = false;
	private boolean gamma_f_table_exists = false;
	
	// prepared statements
	

	
	
	
	/**
	 * Constructor
	 * @param _db the target database this index is made for
	 * @param _location the exact location of the plaintexts within the database
	 */
	public SE_SUISE_Index(DBClient _db, DBLocation _location) {
		
		super(_db);
		
		location = _location;
		
		ks = new KeyspaceState(suiseIndexKeyspace, null, null); // Keyspace object only needed for the index
		ks.setCipherName(suiseIndexKeyspace); // manually set cipher name, so that no keeping track of it is required
		
		String gammafTableName = "gamma_f_" + location.getIdAsPath();
		
		ts_gamma_f = new TableState(gammafTableName, ks, db, null, null, null); // TableStates only needed for this index
		ts_gamma_f.setCipherName(gammafTableName);
		
		// give a rowkey column to be able to process read requests later
		if(location.getTable().getRowkeyColumn().isEncrypted()){
			if(location.getTable().getRowkeyColumn().getType() == ColumnType.STRING) ts_gamma_f.addColumnState(new ColumnState("id", ColumnType.STRING, null, true, true));
			if(location.getTable().getRowkeyColumn().getType() == ColumnType.INTEGER) ts_gamma_f.addColumnState(new ColumnState("id", ColumnType.INTEGER, null, true, true));
			if(location.getTable().getRowkeyColumn().getType() == ColumnType.BYTE) ts_gamma_f.addColumnState(new ColumnState("id", ColumnType.BYTE, null, true, true));
			ts_gamma_f.getColumnByPlainName("id").setCOPEname("id");
		}
		else {
			if(location.getTable().getRowkeyColumn().getType() == ColumnType.STRING) ts_gamma_f.addColumnState(new ColumnState("id", ColumnType.STRING, null, true, false));
			if(location.getTable().getRowkeyColumn().getType() == ColumnType.INTEGER) ts_gamma_f.addColumnState(new ColumnState("id", ColumnType.INTEGER, null, true, false));
			if(location.getTable().getRowkeyColumn().getType() == ColumnType.BYTE) ts_gamma_f.addColumnState(new ColumnState("id", ColumnType.BYTE, null, true, false));
		}
		
		// other column
		ts_gamma_f.addColumnState(new ColumnState("c_quer", ColumnType.BYTE_SET, null, false, false));
		
		String gammawTableName = "gamma_w_" + location.getIdAsPath();
		
		ts_gamma_w = new TableState(gammawTableName, ks, db, null, null, null); // TableStates only needed for this index
		ts_gamma_w.setCipherName(gammawTableName);
		
		// give a rowkey column to be able to process read requests later
		ts_gamma_w.addColumnState(new ColumnState("r_w", ColumnType.BYTE, null, true, false));
		
		// other columns
		if(location.getTable().getRowkeyColumn().getType() == ColumnType.STRING) ts_gamma_w.addColumnState(new ColumnState("i_w", ColumnType.STRING_SET, null, false, false));
		if(location.getTable().getRowkeyColumn().getType() == ColumnType.INTEGER) ts_gamma_w.addColumnState(new ColumnState("i_w", ColumnType.INTEGER_SET, null, false, false));
		if(location.getTable().getRowkeyColumn().getType() == ColumnType.BYTE) ts_gamma_w.addColumnState(new ColumnState("i_w", ColumnType.BYTE_SET, null, false, false));
	
		
		
		
	}
	
	
	
	/**
	 * gets the name of the keyspace where the index is stored in
	 * @return the name of the keyspace where the index is stored in
	 */
	public String getSuiseIndexKeyspace() {
		return suiseIndexKeyspace;
	}
	
	
	
	/**
	 * inserts a row into the index gamma_f
	 * @param identifier the row identifier
	 * @param c_quer c^- as it appears in the paper
	 */										
	public void insertGammafDataSet(DBLocation identifier, HashSet<ByteBuffer> c_quer) {
			
		DBLocation indexId = new DBLocation(ks, ts_gamma_f, null, null); 
						
		if(!gamma_f_table_exists) {
			
			if(!db.cipherKeyspaceExists(indexId)) db.processRequest(new Request(RequestType.CREATE_KEYSPACE, indexId));
			
			gamma_f_createRequest = new Request(RequestType.CREATE_TABLE, indexId);
			
			// id column type dependend from rowkey column type of the original table
			if(identifier.getRowConditions().get(0).getType() == ColumnType.STRING) gamma_f_createRequest.getStringArgs().put("id", "primarykey");
			if(identifier.getRowConditions().get(0).getType() == ColumnType.INTEGER) gamma_f_createRequest.getIntArgs().put("id", 3L);
			if(identifier.getRowConditions().get(0).getType() == ColumnType.BYTE) gamma_f_createRequest.getByteArgs().put("id", "primarykey".getBytes());			
			gamma_f_createRequest.getByteArgs().put("c_quer", "set".getBytes());
			
		}
				
		Request insertRequest = new Request(RequestType.INSERT, indexId);
		if(identifier.getRowConditions().get(0).getType() == ColumnType.BYTE) insertRequest.getByteArgs().put("id", identifier.getRowConditions().get(0).getByteTerm());
		else if(identifier.getRowConditions().get(0).getType() == ColumnType.STRING) insertRequest.getStringArgs().put("id", identifier.getRowConditions().get(0).getStringTerm());
		else if(identifier.getRowConditions().get(0).getType() == ColumnType.INTEGER) insertRequest.getIntArgs().put("id", identifier.getRowConditions().get(0).getLongTerm());
		
		insertRequest.getByteSets().put("c_quer", c_quer);
		
		if(!gamma_f_table_exists) {
			db.insertRow(insertRequest, gamma_f_createRequest);
			gamma_f_table_exists = true;
		}
		else db.processRequest(insertRequest);
	} 
	
	
	
	/**
	 * updates the reversed index gamma_w
	 * @param alpha_f alpha_f as it appears in the paper
	 */													
	public void updateGammawDataSet(SE_SUISE_AddToken alpha_f) {
		
		// create gamma_w, if not exists for that column
		if(!gamma_w_table_exists) {
			if(!db.cipherTableExists(new DBLocation(ks, ts_gamma_w, null, null))) {
		
				Request createRequest = new Request(RequestType.CREATE_TABLE, new DBLocation(ks, ts_gamma_w, null, null));
				createRequest.getByteArgs().put("r_w", "primarykey".getBytes());
			
				ColumnType rowkeyColumnType = null;
				if(alpha_f.getID().getTable().getRowkeyColumn().isEncrypted()) rowkeyColumnType = ColumnType.BYTE;
				else rowkeyColumnType = alpha_f.getID().getTable().getRowkeyColumn().getType();
			
				// I_w column type dependend from rowkey column type of the original table
				if(rowkeyColumnType == ColumnType.STRING) createRequest.getStringArgs().put("i_w", "set");
				if(rowkeyColumnType == ColumnType.INTEGER) createRequest.getIntArgs().put("i_w", 1L);
				if(rowkeyColumnType == ColumnType.BYTE) createRequest.getByteArgs().put("i_w", "set".getBytes());
			
				db.processRequest(createRequest);
				gamma_w_table_exists = true;
			}
		}	
		
		for(int i=0; i<alpha_f.getX().size(); i++){
		
			// check, if row for r_w, if exists
			ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>(); 
			tmpRC.add(new RowCondition("r_w", "=", null, 0L, alpha_f.getX().elementAt(i), ColumnType.BYTE));
			
			Request readRequest = new Request(RequestType.READ, new DBLocation(ks, ts_gamma_w, tmpRC, null));
			
			Result result = db.processRequest(readRequest);
			
			// if not, create one
			if((result == null)||(result.isEmpty())) {				
				createKeyInGammaw(alpha_f.getX().elementAt(i), alpha_f.getID().getRowConditions().get(0).getType());
			}
			
			// update the corresponding set of identifiers
			Request updateRequest = new Request(RequestType.UPDATE_SET, new DBLocation(ks, ts_gamma_w, tmpRC, null));
			
			// identifier dependend from roykey column type
			if(alpha_f.getID().getRowConditions().get(0).getType() == ColumnType.STRING) updateRequest.getStringArgs().put("i_w", alpha_f.getID().getRowConditions().get(0).getStringTerm());
			if(alpha_f.getID().getRowConditions().get(0).getType() == ColumnType.INTEGER) updateRequest.getIntArgs().put("i_w", alpha_f.getID().getRowConditions().get(0).getLongTerm());
			if(alpha_f.getID().getRowConditions().get(0).getType() == ColumnType.BYTE) updateRequest.getByteArgs().put("i_w", alpha_f.getID().getRowConditions().get(0).getByteTerm());
			
			db.processRequest(updateRequest);	
						
		}
		
	}
	
	
	
	/**
	 * creates a new row key for the gamma_w index
	 * @param r_w the new row identifier
	 * @param identifierType type of the new row identifier
	 */
	public void createKeyInGammaw(byte[] r_w, ColumnType identifierType) {
		
		Request insertRequest = new Request(RequestType.INSERT, new DBLocation(ks, ts_gamma_w, null, null));
		
		insertRequest.getByteArgs().put("r_w", r_w);	
		
		if(identifierType == ColumnType.STRING) insertRequest.getStringSets().put("i_w", new HashSet<String>());
		if(identifierType == ColumnType.INTEGER) insertRequest.getIntSets().put("i_w", new HashSet<Long>());
		if(identifierType == ColumnType.BYTE) insertRequest.getByteSets().put("i_w", new HashSet<ByteBuffer>());
		
		db.processRequest(insertRequest);
	}
	
	
	
	/**
	 * updates the gamma_w index
	 * @param r_w the row key of the row to be updated
	 * @param newStringID the new id in case the row key type is of type string
	 * @param newLongID the new id in case the row key type is of type long
	 * @param newByteID the new id in case the row key type is of type byte
	 * @param identifierType the row key type
	 */
	public void updateGammawDataSet(byte[] r_w, String newStringID, long newLongID, byte[] newByteID, ColumnType identifierType) {
		
		ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>();
		tmpRC.add(new RowCondition("r_w", "=", null, 0, r_w, ColumnType.BYTE));
		
		Request updateRequest = new Request(RequestType.UPDATE_SET, new DBLocation(ks, ts_gamma_w, tmpRC, null));
		
		if(identifierType == ColumnType.STRING) updateRequest.getStringArgs().put("i_w", newStringID);
		if(identifierType == ColumnType.INTEGER) updateRequest.getIntArgs().put("i_w", newLongID);
		if(identifierType == ColumnType.BYTE) updateRequest.getByteArgs().put("i_w", newByteID);
		
		db.processRequest(updateRequest);
		
	}
	
	
	
	/**
	 * Checks if a certain key is present in the gamma_w index
	 * @param r_w the key to check for
	 * @return the result of the query for the key that was checked for
	 */
	public Result checkForSearchToken(byte[] r_w) {
	
		ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>();
		tmpRC.add(new RowCondition("r_w", "=", null, 0L, r_w, ColumnType.BYTE));
		
		ArrayList<String> tmpColumns= new ArrayList<String>();
		tmpColumns.add("i_w");
		
		Request readRequest = new Request(RequestType.READ_WITHOUT_IV, new DBLocation(ks, ts_gamma_w, tmpRC, tmpColumns));
		return db.processRequest(readRequest);
	}
	
	
	
	/**
	 * gets the gamma_f index
	 * @return the gamma_f index
	 */
	public Result getGammaf() {
		
		ArrayList<String> tmpColumns = new ArrayList<String>();
		tmpColumns.add("c_quer");
		
		Request readRequest = new Request(RequestType.READ_WITHOUT_IV, new DBLocation(ks, ts_gamma_f, new ArrayList<RowCondition>(), tmpColumns));		
		return db.processRequest(readRequest);
		
	}
		
}
