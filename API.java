import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
//import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.DOMBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.xml.sax.SAXException;

import crypto.KeyStoreManager;
import crypto.PRG;
import misc.Misc;
import misc.Timer;
import databases.ColumnState;
import databases.DBClient;
import databases.DBClientFactory;
import databases.DBLocation;
import databases.DecryptedResults;
import databases.KeyspaceState;
import databases.Request;
import databases.Result;
import databases.RowCondition;
import databases.TableState;
import enums.TableProfile;
import enums.ColumnType;
import enums.DatabaseType;
import enums.DistributionProfile;
import enums.RequestType;

class API {
	
	// path to the xml config file this API is using
	private String configFilePath = "";
	
	// a list of keyspaces maintained by this configuration
	private ArrayList<KeyspaceState> keyspaces = new ArrayList<KeyspaceState>();
	
	// the used keystore
	private KeyStoreManager keystore;
	
	// for turning on/off status output 
	private boolean silent;
	
	
	
	
	
	/**
	 * returns a KeyspaceState by its plaintext name, null if not exists
	 * @param _name the desired plaintext name 
	 * @return a KeyspaceState object with the desired plaintext name
	 */
	private KeyspaceState getKeyspaceByName(String _name) {
		
		for(KeyspaceState ks : keyspaces) if (ks.getPlainName().equals(_name)) return ks;
		
		return null;
	}
	
	
	
	/**
	 * Initializes the Interface, reads in the metadata stored in the config file and initializes all objects
	 * @param path the path to the config file
	 * @param silent turns on/off console output
	 */
	public API(String _path, String _password, boolean _silent) {
		
		// initialize
		
		configFilePath = _path;
		silent = _silent;
		if(!silent) Misc.printStatus("Initializing...");
		
		// load keystore
		keystore = new KeyStoreManager(configFilePath + ".jks", _password);
		
		// if there is a file present at this path
		// read in meta data keyspace by keyspace
		File configFile = new File(configFilePath);
		if(configFile.exists() && !configFile.isDirectory()) { 
		    
			if(!silent) Misc.printStatus("Loading configuration from \"" + configFilePath + "\"...");
			
			// create the w3c DOM document from which JDOM is to be created
	        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

	        org.w3c.dom.Document w3cDocument;
			try {
				DocumentBuilder dombuilder = factory.newDocumentBuilder();
				w3cDocument = dombuilder.parse(_path);
				
				DOMBuilder jdomBuilder = new DOMBuilder();
		        Document doc = jdomBuilder.build(w3cDocument);
		        
		        Element configurationRoot = doc.getRootElement();
		        
		        // parsing the keyspaces
		        Element configurationKeyspaces = configurationRoot.getChild("keyspaces");
		        
		        List<Element> kss = configurationKeyspaces.getChildren("keyspace");
		        for(Element ks : kss) keyspaces.add(new KeyspaceState(ks, keystore));
		        
		        if(!silent) Misc.printStatus(keyspaces.size() + " keyspace(s) sucessfully loaded");
		        
		        
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}        
			
		}
		
		// if not, start a new database and save everything to
		// that path in the end
		else {
			if(!silent) Misc.printStatus("No configuration found at \"" + configFilePath + "\", starting a new one...");
				
		}
	}
	
	
	
	/**
	 * adds a keyspace 
	 * @param _keyspaceName the plaintext name of the new keyspace
	 * @param _dbs the available database connections for the new keyspace, given as "name->ip"
	 * @param _keyspaceParameters option to pass database intern parameters to the keyspace creation process 
	 * @param _password the password used for the keystore of this keyspace
	 */
	public void addKeyspace(String _keyspaceName, String[] _dbs, HashMap<String, String> _keyspaceParameters, String _password) {
				
		//check if keyspace exists already
		if(getKeyspaceByName(_keyspaceName) != null) {
			if(!silent) Misc.printStatus("Keyspace \"" + _keyspaceName + "\" already exists!");
		}
		
		else {
			//parse database strings and initialize DBClients
			ArrayList<DBClient> dbs = new ArrayList<DBClient>(); 
		
			for(String s : _dbs) {
				String[] components = s.split("->");
				if(components.length == 2) dbs.add(DBClientFactory.getDBClient(DatabaseType.valueOf(components[0].toUpperCase()), components[1]));
			}
		
			//initialize keyspace state
			KeyspaceState ks = new KeyspaceState(_keyspaceName, dbs, new KeyStoreManager(configFilePath + ".jks", _password));
			keyspaces.add(ks);
		
			//write keyspace to all chosen databases
			DBLocation id = new DBLocation(ks, null, null, null);
			Request createKeyspaceRequest = new Request(RequestType.CREATE_KEYSPACE, id);
			createKeyspaceRequest.setStringArgs(_keyspaceParameters); // directly set the parameters
		
			for(DBClient db : ks.getAvailableDatabases()) {
				db.processRequest(createKeyspaceRequest);
				if(!silent) Misc.printStatus("Keyspace \"" + _keyspaceName + "\" created on " + db);
			}
		}
		
	}
	
	
	
	/**
	 * adds a table to an existing keyspace
	 * @param _keyspace the keyspace name
	 * @param _tablename the name of the new table
	 * @param _profile the profile of the new table
	 * @param _columns the column names and types
	 * @return 0 if the table was successfully created, 1 otherwise
	 */
	public int addTable(String _keyspace, String _tablename, TableProfile _profile, DistributionProfile _distribution, String[] _columns) {
		
	
		//check if specified keyspace exits	
		KeyspaceState keyspace = getKeyspaceByName(_keyspace); 
		if(keyspace == null) {
			if(!silent) Misc.printStatus("Keyspace \"" + _keyspace + "\" does not exist!");
		}
		//if so
		else {
			int numberOfColumns = _columns.length;
			
			//parse columns
			String[][] parsedColumns = new String[numberOfColumns][];
			
			int numberOFExpectedColumnAttributes = 3;
			if(_distribution == DistributionProfile.CUSTOM) numberOFExpectedColumnAttributes = 4;
			
			
			int i = 0;
			for(String s : _columns) {
				parsedColumns[i] = s.split("->");
				
				if(parsedColumns[i].length < numberOFExpectedColumnAttributes) {
					if(!silent) Misc.printStatus("Unable to parse definition of column " + _keyspace + "." + _tablename);
					return 1;
				}
				
				i++;
			}
			
			// build ColumnState objects(s)
			int numberOfAvailableDBs = keyspace.getAvailableDatabases().size();
			PRG g = new PRG(); 
				
			ColumnState[] columnStates = new ColumnState[_columns.length]; 
			Integer[] databaseMapping = new Integer[_columns.length]; //stores the index of the desired DB for every column in case of DistributionProfile.CUSTOM
			
			// parse columns
			i = 0;
			int rowKeyCounter = 0;
			for(String[] parsedColumnString : parsedColumns) {		
							
				boolean isRowkey = false;
				if(parsedColumnString.length > numberOFExpectedColumnAttributes) { 
					if(parsedColumnString[numberOFExpectedColumnAttributes].equals("rowkey")) {
						isRowkey = true;
						rowKeyCounter++;
					}
				}
				
				boolean isEncrypted = false;
				if(parsedColumnString[numberOFExpectedColumnAttributes - 3].equals("encrypted".toLowerCase())) isEncrypted = true;
				
				columnStates[i] = new ColumnState(parsedColumnString[numberOFExpectedColumnAttributes - 1], 
												ColumnType.valueOf(parsedColumns[i][numberOFExpectedColumnAttributes - 2].toUpperCase()), 
												_profile, 
												isRowkey,
												isEncrypted);
				
				if(columnStates[i].isRowkeyColumn() && ((columnStates[i].getType() == ColumnType.STRING_SET)||(columnStates[i].getType() == ColumnType.INTEGER_SET)||(columnStates[i].getType() == ColumnType.BYTE_SET))) {
					if(!silent) Misc.printStatus("Collections are not allowed as row key columns (" + columnStates[i].getPlainName() + ")");
					return 1;
				}
				
				// store desired DB in case of DistributionProfile.CUSTOM
				if(_distribution == DistributionProfile.CUSTOM) {
					
					if(Integer.valueOf(parsedColumnString[0]) > numberOfAvailableDBs) {
						if(!silent) Misc.printStatus("Only " + numberOfAvailableDBs + " DB connections available for keyspace \"" + _keyspace + "\" (you specified DB#" + parsedColumnString[0] + " for column \"" + columnStates[i].getPlainName() + "\")");
						return 1;
					}
					else databaseMapping[i] = Integer.valueOf(parsedColumnString[0]);
				}
				
				i++;						
			}
			
			// make sure there is really exactly one rowkey column specified
			if(rowKeyCounter != 1) {
				if(!silent) Misc.printStatus("Too much or less rowkey columns specified for table " + _keyspace + "." + _tablename);
				return 1;
			}
								
			// build TableState object(s)
			TableState[] tableStates = new TableState[numberOfAvailableDBs];
				
			// generate the tables common iv and key
			byte[] iv = g.generateRandomPrintableBytes(16);		// AES IV	
			byte[] key = g.generateRandomPrintableBytes(32); 	// AES key for DET encryption
				
			for(i=0; i<numberOfAvailableDBs; i++) {
			
				tableStates[i] = new TableState(_tablename, keyspace, keyspace.getAvailableDatabases().get(i), _profile, iv, key);
				
				// attach the new table(s) to the keyspace
				keyspace.addTableState(tableStates[i]);	
			}
			
			// attach columns to tables
			for(int j=0; j<columnStates.length; j++) {
				
				// the rowkey column has to be attached to every table
				if(columnStates[j].isRowkeyColumn()) {
					for(int k=0; k<tableStates.length; k++) {
						tableStates[k].addColumnState(columnStates[j]);
					}
				}
				// all non-rowkey columns can be distributed using different approaches
				else{
					// randomly
					if(_distribution == DistributionProfile.RANDOM) tableStates[g.generateRandomInt(0, numberOfAvailableDBs - 1)].addColumnState(columnStates[j]);
					
					// round robin
					if(_distribution == DistributionProfile.ROUNDROBIN) tableStates[j%numberOfAvailableDBs].addColumnState(columnStates[j]);
					
					// user defined
					if(_distribution == DistributionProfile.CUSTOM) tableStates[databaseMapping[j] - 1].addColumnState(columnStates[j]);
				}
			}
				
			// write TableStates to databases
			for(TableState ts : tableStates) {
				
				
				int numberOfColumnsInTable = 1; // the IV column
				
				for(ColumnState cs : ts.getAvailableColumns()) {
				
					if(cs.isEncrypted()) { // if the column is encrypted
						if(cs.isRowkeyColumn()){
							if((cs.getType() == ColumnType.STRING)) numberOfColumnsInTable += 3; 		//det + ope + se
							else if((cs.getType() == ColumnType.INTEGER)) numberOfColumnsInTable += 1;  //ope
							//else numberOfColumnsInTable += 1; 																			    //ope | byte not allowed
						}
						else
						{
							if(cs.getType() == ColumnType.STRING) numberOfColumnsInTable += 3; 		 	//det + ope + se
							else if(cs.getType() == ColumnType.STRING_SET) numberOfColumnsInTable += 2; //det + se
							else if(cs.getType() == ColumnType.INTEGER) numberOfColumnsInTable += 2;	//det + ope
							else if(cs.getType() == ColumnType.INTEGER_SET) numberOfColumnsInTable += 1;//det
							else if(cs.getType() == ColumnType.BYTE)numberOfColumnsInTable += 1; 		//det
							else if(cs.getType() == ColumnType.BYTE_SET)numberOfColumnsInTable += 1; 	//det
						}
					}
					else { // if the column is not encrypted
						
						numberOfColumnsInTable += 1; // no onion layers required, always exact one column necessary
													 // whether row key or not
						
					}
				}
				
				DBLocation id = new DBLocation(ts.getKeyspace(), ts, null, null);
				
				Request createRequest = new Request(RequestType.CREATE_TABLE, id);
				
				// save the IV as byte array
				createRequest.getByteArgs().put(ts.getIVcolumnName(), "null".getBytes()); // the IV column first, ...
				
				// ...then the others
				for(ColumnState cs : ts.getAvailableColumns()) {
					
					if(cs.isEncrypted()) { // if the column is encrypted, add all the necessary 
										   // layer columns to the create request
					
						if(cs.isRowkeyColumn()) {
							// row keys can't be collection type collumns and need no DET column
							if(cs.getType() == ColumnType.STRING) {
								createRequest.getByteArgs().put(cs.getCOPEname(), "primarykey".getBytes());
								createRequest.getByteArgs().put(cs.getCDETname(), "null".getBytes());
								createRequest.getStringArgs().put(cs.getCSEname(), "null");
							}
						
							if(cs.getType() == ColumnType.INTEGER) {
								createRequest.getIntArgs().put(cs.getCOPEname(), (long)3);						
							}
						
							/*if(cs.getType() == ColumnType.BYTE) {							
								createRequest.getByteArgs().put(cs.getCOPEname(), "primarykey".getBytes());
							}*/
						}
						else {
							if(cs.getType() == ColumnType.STRING) {
								createRequest.getByteArgs().put(cs.getCDETname(), "null".getBytes());
								createRequest.getByteArgs().put(cs.getCOPEname(), "null".getBytes());
								createRequest.getStringArgs().put(cs.getCSEname(), "null");
							}
						
							if(cs.getType() == ColumnType.STRING_SET) {
								createRequest.getByteArgs().put(cs.getCDETname(), "set".getBytes());
								createRequest.getStringArgs().put(cs.getCSEname(), "set");
							}
						
							if(cs.getType() == ColumnType.INTEGER) {
								createRequest.getByteArgs().put(cs.getCDETname(), "null".getBytes());
								createRequest.getByteArgs().put(cs.getCOPEname(), "null".getBytes());						
							}
						
							if(cs.getType() == ColumnType.INTEGER_SET) {
								createRequest.getByteArgs().put(cs.getCDETname(), "set".getBytes());						
							}
						
							if(cs.getType() == ColumnType.BYTE) {
								createRequest.getByteArgs().put(cs.getCDETname(), "null".getBytes());					
							}
						
							if(cs.getType() == ColumnType.BYTE_SET) {
								createRequest.getByteArgs().put(cs.getCDETname(), "set".getBytes());								
							}
						}
					}
					
					else { // if the column is not encrypted, only add one column with the plaintext name and original data type
						
						if(cs.isRowkeyColumn()) {
							if(cs.getType() == ColumnType.STRING) createRequest.getStringArgs().put(cs.getPlainName(),  "primarykey");
							if(cs.getType() == ColumnType.INTEGER) createRequest.getIntArgs().put(cs.getPlainName(), (long)3);
							//ColumnType.BYTE not allowed here
						}
						else {
							if(cs.getType() == ColumnType.STRING) createRequest.getStringArgs().put(cs.getPlainName(), "null");	
							if(cs.getType() == ColumnType.STRING_SET) createRequest.getStringArgs().put(cs.getPlainName(), "set");
									
							if(cs.getType() == ColumnType.INTEGER) createRequest.getIntArgs().put(cs.getPlainName(), (long) 0);						
							if(cs.getType() == ColumnType.INTEGER_SET) createRequest.getIntArgs().put(cs.getPlainName(), (long) 1);						
							
							if(cs.getType() == ColumnType.BYTE) createRequest.getByteArgs().put(cs.getPlainName(), "null".getBytes());
							if(cs.getType() == ColumnType.BYTE_SET) createRequest.getByteArgs().put(cs.getPlainName(), "set".getBytes());
								
						}
						
					}
					
				}
					
				// perform the actual writing step
				ts.getDBClient().processRequest(createRequest);
				
			}
		}
		
		return 0;
	}
	
	
	
	/**
	 * Inserts a data row into a (logical) table
	 * @param keyspace the tables keyspace
	 * @param table the tables name
	 * @param stringData the new string data
	 * @param intData the new numerical data
	 * @param byteData the new byte blob data
	 * @return the time needed by the database to perform the insertion(s)
	 */
	public long insertRow(String keyspace, String table, 
			HashMap<String, String> stringData, HashMap<String, Long> intData, HashMap<String, byte[]> byteData,  // "regular" values
			HashMap<String, HashSet<String>> stringSetData, HashMap<String, HashSet<Long>> intSetData, HashMap<String, HashSet<ByteBuffer>> byteSetData) { // collections
		
		long runtime = 0;
		
		// get references to all physical tables involved
		TableState[] physTables = getKeyspaceByName(keyspace).getTableByPlainName(table);
		
		if(physTables == null) {
			if(!silent) Misc.printStatus("Can't insert a row into table \"" + keyspace + "." + table + "\", because it doesn't exist.");
			return runtime;
		}
				
		// check if the rowkey was specified
		
		// cycle through the data hashmaps and look for the rowkey column name
		boolean rowkeyProvided = false;
		
		// find out about the rowkey column and create a row condition referring to it...
		ColumnState rowkeyColumn = physTables[0].getRowkeyColumn(); // pick the first table since the rowkey column is everywhere the same
		
		RowCondition rowkeyRowCondition = null;
		byte[] encryptedStringRowkey = null;
		byte[] encryptedIntRowkey = null;
		
		// ...for unencrypted rowkeys
		if(!rowkeyColumn.isEncrypted()) {
			for(String s : stringData.keySet()) 
				if(s.equals(rowkeyColumn.getPlainName())) {
					rowkeyProvided = true;
					//stringRowkey = stringData.get(rowkeyColumnName);
					rowkeyRowCondition = new RowCondition(rowkeyColumn.getPlainName(), "=", stringData.get(rowkeyColumn.getPlainName()), 0, null, ColumnType.STRING);
				}
		
			if(!rowkeyProvided) 
				for(String s : intData.keySet()) 
					if(s.equals(rowkeyColumn.getPlainName())) {
						rowkeyProvided = true;
						//intRowkey = intData.get(rowkeyColumnName);
						rowkeyRowCondition = new RowCondition(rowkeyColumn.getPlainName(), "=", null, intData.get(rowkeyColumn.getPlainName()), null, ColumnType.INTEGER);
					}
		
			if(!rowkeyProvided) 
				for(String s : byteData.keySet()) 
					if(s.equals(rowkeyColumn.getPlainName())) {
						rowkeyProvided = true;
						//byteRowkey = byteData.get(rowkeyColumnName);
						rowkeyRowCondition = new RowCondition(rowkeyColumn.getPlainName(), "=", null, 0, byteData.get(rowkeyColumn.getPlainName()), ColumnType.BYTE);
					}	
		}
		// ...for encrypted rowkeys
		else {		
			
			ArrayList<String> tmp = new ArrayList<String>();
			tmp.add(rowkeyColumn.getPlainName());
			
			// pre-encrypt rowkey, only strings and ints allowed as rowkey
			if(rowkeyColumn.getType() == ColumnType.STRING) {
				
				encryptedStringRowkey = rowkeyColumn.getOPEScheme().encryptString(stringData.get(rowkeyColumn.getPlainName()), new DBLocation(rowkeyColumn.getTable().getKeyspace(), rowkeyColumn.getTable(), null, tmp));
				rowkeyRowCondition = new RowCondition(rowkeyColumn.getCOPEname(), "=", null, 0, encryptedStringRowkey, ColumnType.BYTE);
			
				rowkeyProvided = true;
			}
			
			if(rowkeyColumn.getType() == ColumnType.INTEGER) {
				
				encryptedIntRowkey = Misc.longToBytes(rowkeyColumn.getOPEScheme().encrypt(intData.get(rowkeyColumn.getPlainName()), new DBLocation(rowkeyColumn.getTable().getKeyspace(), rowkeyColumn.getTable(), null, tmp)));
				rowkeyRowCondition = new RowCondition(rowkeyColumn.getCOPEname(), "=", null, 0, encryptedStringRowkey, ColumnType.BYTE);
			
				rowkeyProvided = true;
			}
			
		}
		
		
		if (!rowkeyProvided) {
			if(!silent) Misc.printStatus("No rowkey specified (\"" + rowkeyColumn.getPlainName() + "\" field is missing)");
			return runtime;
		}
		
		
		// if rowkey column was specified, continue
		else {
			
			// make up the row's IV for RND encryption
			byte[] iv = new PRG().generateRandomBytes(16);
			
			// build insertRequests for all physical tables
			for(TableState physTable : physTables) {
			
				//System.out.println("physTable="+physTable.getPlainName());
				Request insertRequest = new Request(RequestType.INSERT, new DBLocation(physTable.getKeyspace(), physTable, null, null));
				
				// insert IV
				insertRequest.getByteArgs().put(physTable.getIVcolumnName(), iv);
				
				// for all values in the data array, that have corresponding columns in this phy table,
				// generate their ciphertext columns
				for(ColumnState cs : physTable.getAvailableColumns()) {
				
					cs.adjustColumnToTable(physTable);
			
					if(cs.getType() == ColumnType.STRING) {
						
						String value = stringData.get(cs.getPlainName());
						
						if(value != null) { // if value was specified in the query
						
							if(cs.isEncrypted()) { // if column is to be stored in encrypted form
							
								// Encrypt
								ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>();
								tmpRC.add(rowkeyRowCondition);//new RowCondition(physTable.getRowkeyColumnName(),"=",stringRowkey, intRowkey, byteRowkey, physTable.getRowkeyColumn().getType()));
								
								ArrayList<String> tmpSEColumns = new ArrayList<String>();
								tmpSEColumns.add(cs.getPlainName()); 
								
								ArrayList<String> tmpOPEColumns = new ArrayList<String>();
								tmpOPEColumns.add(cs.getPlainName()); // Plain column name for the client side index
								
								String encryptedSEValue  = cs.getSEScheme() .encrypt(      value , new DBLocation(physTable.getKeyspace(), physTable, tmpRC, tmpSEColumns));
								// byte[] encryptedOPEValue = cs.getOPEScheme().encryptString(value , new DBLocation(physTable.getKeyspace(), physTable, tmpRC, tmpOPEColumns));   // veralteter Kommentar: bei OPE können die Plain IDs benutzt werden, da deren Indexe eh nur auf dem Client liegen, ist auch wichtig, damit rowkey spalten auf verschiedenen Datenbanken den gleichen Index benutzten
								//byte[] encryptedOPEValue = Misc.longToBytes(cs.getOPEScheme().encrypt(Misc.stringToLong(value), new DBLocation(physTable.getKeyspace(), physTable, tmpRC, tmpOPEColumns)));
								byte[] encryptedOPEValue = Misc.longArrayListToByteArray(cs.getOPEScheme().encryptList(Misc.stringToLongArrayList(value), new DBLocation(physTable.getKeyspace(), physTable, tmpRC, tmpOPEColumns)));
								
								byte[] encryptedDETValue = cs.getDETScheme().encrypt(      value.getBytes());
							
								byte[] encryptedRNDOPEValue = null;
								if(!cs.isRNDoverOPEStrippedOff()) encryptedRNDOPEValue = cs.getRNDScheme().encrypt(encryptedOPEValue , iv);
								byte[] encryptedRNDDETValue = null;
								if(!cs.isRNDoverDETStrippedOff()) encryptedRNDDETValue = cs.getRNDScheme().encrypt(encryptedDETValue , iv);
							
								// SE column
								insertRequest.getStringArgs().put(cs.getCSEname(), encryptedSEValue);
						
								// OPE column	
								// store without RND Layer if rowkey column, otherwise the datamodel would be broken
								if(cs.isRowkeyColumn()) 			  insertRequest.getByteArgs().put(cs.getCOPEname(), encryptedStringRowkey);
								else if(cs.isRNDoverOPEStrippedOff()) insertRequest.getByteArgs().put(cs.getCOPEname(),     encryptedOPEValue);
								else                                  insertRequest.getByteArgs().put(cs.getCOPEname(),  encryptedRNDOPEValue);
						
								// DET column
								if(cs.isRNDoverDETStrippedOff()) insertRequest.getByteArgs().put(cs.getCDETname(), encryptedDETValue);
								else                             insertRequest.getByteArgs().put(cs.getCDETname(), encryptedRNDDETValue);
						
							}
							else { //unencrypted								
								insertRequest.getStringArgs().put(cs.getPlainName(), value);								
							}
						}
					}
									
					if(cs.getType() == ColumnType.STRING_SET) {
							
						HashSet<String> value = stringSetData.get(cs.getPlainName());
						
						if(value != null) { // if value was specified in the query
						
							if(cs.isEncrypted()) { // if column is to be stored in encrypted form
							
								// Encrypt
								ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>();
								tmpRC.add(rowkeyRowCondition);//new RowCondition(physTable.getRowkeyColumnName(),"=",stringRowkey, intRowkey, byteRowkey, physTable.getRowkeyColumn().getType()));
								
								ArrayList<String> tmpColumns = new ArrayList<String>();
								tmpColumns.add(cs.getPlainName());
								
								HashSet<String> encryptedSEValue  = cs.getSEScheme(). encryptSet(                                   value,  new DBLocation(physTable.getKeyspace(), physTable, tmpRC, tmpColumns));
								
								HashSet<byte[]> encryptedDETValue  = cs.getDETScheme().encryptByteSet(Misc.StringHashSet2ByteHashSet(value));
								
								HashSet<byte[]> encryptedRNDDETValue = null;
								if(!cs.isRNDoverDETStrippedOff()) encryptedRNDDETValue = cs.getRNDScheme().encryptByteSet(encryptedDETValue, iv);
								
								// SE column
								insertRequest.getStringSets().put(cs.getCSEname(), encryptedSEValue);
						
								// RND or DET column
								if(!cs.isRNDoverDETStrippedOff()) insertRequest.getByteSets().put(cs.getCDETname(), Misc.byteHashSet2ByteBufferHashSet(encryptedRNDDETValue));
								else insertRequest.getByteSets().put(cs.getCDETname(), Misc.byteHashSet2ByteBufferHashSet(encryptedRNDDETValue));
						
							}
							else { //unencrypted								
								insertRequest.getStringSets().put(cs.getPlainName(), value);								
							}
						}
					}
					
					if((cs.getType() == ColumnType.INTEGER)) {
						
						long value = intData.get(cs.getPlainName());
						
						if(cs.isEncrypted()) { // if column is to be stored in encrypted form
						
							// Encrypt
							ArrayList<String> tmpColumns = new ArrayList<String>();
							tmpColumns.add(cs.getPlainName()); // Plain column name for the client side index
							
							long   encryptedOPEValue = cs.getOPEScheme().encrypt(value, new DBLocation(physTable.getKeyspace(), physTable, null, tmpColumns));
							byte[] encryptedDETValue = cs.getDETScheme().encrypt(Misc.longToBytes(value));
						
							byte[] encryptedRNDOPEValue = null;
							if(!cs.isRNDoverOPEStrippedOff()) encryptedRNDOPEValue = cs.getRNDScheme().encrypt(Misc.longToBytes(encryptedOPEValue), iv);
							byte[] encryptedRNDDETValue = null;
							if(!cs.isRNDoverDETStrippedOff()) encryptedRNDDETValue = cs.getRNDScheme().encrypt(encryptedDETValue, iv);
						
							// OPE column						
							// store without RND Layer if rowkey column, otherwise the datamodel would be broken
							if(cs.isRowkeyColumn())				  insertRequest.getByteArgs().put(cs.getCOPEname(), encryptedIntRowkey                 );
							else if(cs.isRNDoverOPEStrippedOff()) insertRequest.getByteArgs().put(cs.getCOPEname(), Misc.longToBytes(encryptedOPEValue));
							else                                  insertRequest.getByteArgs().put(cs.getCOPEname(), encryptedRNDOPEValue               );

							// DET column (only if not a rowkey column)
							if(!cs.isRowkeyColumn()) {
								if(cs.isRNDoverDETStrippedOff()) insertRequest.getByteArgs().put(cs.getCDETname(), encryptedDETValue   );
								else                             insertRequest.getByteArgs().put(cs.getCDETname(), encryptedRNDDETValue);
							}
						}
						else { //unencrypted								
							insertRequest.getIntArgs().put(cs.getPlainName(), value);								
						}
						
					}
					
					if(cs.getType() == ColumnType.INTEGER_SET) {
						
						HashSet<Long> value = intSetData.get(cs.getPlainName());
						
						if(value != null) { // if value was specified in the query
						
							if(cs.isEncrypted()) { // if column is to be stored in encrypted form
							
								// Encrypt
								ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>();
								tmpRC.add(new RowCondition(physTable.getRowkeyColumnName(),"","",0, null, ColumnType.INTEGER_SET));
								
								ArrayList<String> tmpColumns = new ArrayList<String>();
								tmpColumns.add(cs.getPlainName());
								
								HashSet<byte[]> encryptedDETValue  = cs.getDETScheme().encryptByteSet(Misc.LongHashSet2ByteHashSet(value));
								
								HashSet<byte[]> encryptedRNDDETValue = null;
								if(!cs.isRNDoverDETStrippedOff()) encryptedRNDDETValue = cs.getRNDScheme().encryptByteSet(encryptedDETValue, iv);
								
								if(!cs.isRNDoverDETStrippedOff()) insertRequest.getByteSets().put(cs.getCDETname(), Misc.byteHashSet2ByteBufferHashSet(encryptedRNDDETValue));
								else insertRequest.getByteSets().put(cs.getCDETname(), Misc.byteHashSet2ByteBufferHashSet(encryptedDETValue));		
							}
							else { //unencrypted								
								insertRequest.getIntSets().put(cs.getPlainName(), value);								
							}
						}
					}
					
					if((cs.getType() == ColumnType.BYTE)) {
						
						byte[] value = byteData.get(cs.getPlainName());
							
						if(value != null) { // if value was specified in the query
						
							if(cs.isEncrypted()) { // if column is to be stored in encrypted form
							
								// Encrypt
								byte[] encryptedDETValue = cs.getDETScheme().encrypt(String.valueOf(value).getBytes());
								byte[] encryptedRNDDETValue = null;
								if(!cs.isRNDoverDETStrippedOff()) encryptedRNDDETValue = cs.getRNDScheme().encrypt(encryptedDETValue, iv);
							
								// DET column (only if not a rowkey column)
								if(!cs.isRowkeyColumn()) {
									if(cs.isRNDoverDETStrippedOff()) insertRequest.getByteArgs().put(cs.getCDETname(), encryptedDETValue   );
									else                             insertRequest.getByteArgs().put(cs.getCDETname(), encryptedRNDDETValue);
								}
							}
							else { //unencrypted								
								insertRequest.getByteArgs().put(cs.getPlainName(), value);								
							}
						}
					}
					
					if(cs.getType() == ColumnType.BYTE_SET) {
						
						HashSet<ByteBuffer> value = byteSetData.get(cs.getPlainName());
						
						if(value != null) { // if value was specified in the query
						
							if(cs.isEncrypted()) { // if column is to be stored in encrypted form
							
								// Encrypt
								ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>();
								tmpRC.add(new RowCondition(physTable.getRowkeyColumnName(),"","",0, null, ColumnType.BYTE_SET));
								
								ArrayList<String> tmpColumns = new ArrayList<String>();
								tmpColumns.add(cs.getPlainName());
								
								HashSet<byte[]> encryptedDETValue  = cs.getDETScheme().encryptByteSet(Misc.byteBufferHashSet2ByteHashSet(value));
								
								HashSet<byte[]> encryptedRNDDETValue = null;
								if(!cs.isRNDoverDETStrippedOff()) encryptedRNDDETValue = cs.getRNDScheme().encryptByteSet(encryptedDETValue, iv);
								
								
								// RND column
								if(!cs.isRNDoverDETStrippedOff()) insertRequest.getByteSets().put(cs.getCDETname(), Misc.byteHashSet2ByteBufferHashSet(encryptedRNDDETValue));	
								else insertRequest.getByteSets().put(cs.getCDETname(), Misc.byteHashSet2ByteBufferHashSet(encryptedDETValue));		
							}
							else { //unencrypted								
								insertRequest.getByteSets().put(cs.getPlainName(), value);								
							}
						}
					}
				}
				
				// finally write the data to the table
				
				runtime += physTable.getDBClient().processRequest(insertRequest).getRuntime();
				
			}
			
			return runtime;
		}	
	
	}
		
	
	/**
	 * Executes a query
	 * @param _columns the columns that shall be part of the result set
	 * @param _keyspace the keyspace of the table to be queried
	 * @param _table the table to be queried
	 * @param _conditions the conditions, that restrict the 
	 */
	public DecryptedResults query (String[] _columns, String _keyspace, String _table, String[] _conditions) {
		
		Timer t = new Timer();
		t.start();
		
		// checken, ob es keyspace und table überaupt gibt
		if(getKeyspaceByName(_keyspace) == null) {
			if(!silent) Misc.printStatus("Keyspace \"" + _keyspace + "\" doesn't exist. Query aborted.");
			return null;
		}
		
		if(getKeyspaceByName(_keyspace).getTableByPlainName(_table) == null) {
			if(!silent) Misc.printStatus("Table \"" + _keyspace + "." + _table + "\" doesn't exist. Query aborted.");
			return null;
		}
		
		// conditions zu RowConditions umbauen und direkt den betroffenen ColumnState suchen		
		HashMap<RowCondition, ColumnState> conditions = new HashMap<RowCondition, ColumnState>();
		
		// TODO: das schöner lösen
		if(_conditions == null) _conditions = new String[]{}; // Methodenaufruf mit null ermöglichen, falls keine RowConditions benötigt werden
		
		//RowCondition[] condition = new RowCondition[_condition.length]; 
		for(int i=0; i<_conditions.length; i++) {
			
			String comparator = null;
			String[] arg = null;
			
			comparator = "=";
			arg = _conditions[i].split(comparator);
			if(arg.length != 2) {
				comparator = "<";
				arg = _conditions[i].split(comparator);
				if(arg.length != 2) {
					comparator = ">";
					arg = _conditions[i].split(comparator);
					if(arg.length != 2) {
						comparator = "#";
						arg = _conditions[i].split(comparator);
						if(arg.length != 2) {
							if(!silent) Misc.printStatus("Unable to recognize comparator in " + _conditions[i]);
							return null;
						}
					}
				}
			}
							
			// find out column type
			ColumnState tmpCS = null;
						
			TableState[] tables = getKeyspaceByName(_keyspace).getTableByPlainName(_table); // look at possible phys tables
			for(TableState ts : tables) if(ts.getColumnByPlainName(arg[0]) != null) tmpCS = ts.getColumnByPlainName(arg[0]); // look everywhere for a column with the given name
			if(tmpCS == null) { // if type is still == null, no column was found 
				if(!silent) Misc.printStatus("Unable to find column " + _keyspace + "." + _table + "." + arg[0] + " in metadata");
				return null;
			}
			
			// compose RowCondition
			if(tmpCS.getType() == ColumnType.STRING) conditions.put(new RowCondition(arg[0], comparator, arg[1], 0, null, tmpCS.getType()), tmpCS); 
			if(tmpCS.getType() == ColumnType.INTEGER) conditions.put(new RowCondition(arg[0], comparator, null, Long.valueOf(arg[1]), null, tmpCS.getType()), tmpCS); 
			if(tmpCS.getType() == ColumnType.BYTE) conditions.put(new RowCondition(arg[0], comparator, null, 0, arg[1].getBytes(), tmpCS.getType()), tmpCS);
			if(tmpCS.getType() == ColumnType.STRING_SET&&comparator.equals("=")) {
				for (String s : arg[1].split(",")) {
					conditions.put(new RowCondition(arg[0], " CONTAINS ", s.trim(), 0, null, tmpCS.getType()), tmpCS); 
				}
			}
			if(tmpCS.getType() == ColumnType.INTEGER_SET&&comparator.equals("=")) {
				for (String s : arg[1].split(",")) {
					conditions.put(new RowCondition(arg[0], " CONTAINS ", "", Long.valueOf(s.trim()), null, tmpCS.getType()), tmpCS); 
				}
			}
			if(tmpCS.getType() == ColumnType.BYTE_SET&&comparator.equals("=")) {
				for (String s : arg[1].split(",")) {
					conditions.put(new RowCondition(arg[0], " CONTAINS ", "", 0, s.trim().getBytes(), tmpCS.getType()), tmpCS); 
				}
			}
			
			
		}
				
		// in allen Condition-involvierten spalten gucken, ob RND layer runter müssen
		
		for(RowCondition rc : conditions.keySet()) {
			ColumnState cs = conditions.get(rc);
			
			// RND layer removal only necessary, if the column was stored encrypted
			if(cs.isEncrypted()) {
				
				// bei bedarf onion layer abbauen
				if((rc.getComparator().equals("="))&&(!cs.isRNDoverDETStrippedOff())) cs.getTable().getDBClient().removeRNDLayer(cs, "DET");
				if((rc.getComparator().equals(" CONTAINS "))&&(!cs.isRNDoverDETStrippedOff())) cs.getTable().getDBClient().removeRNDLayer(cs, "DET");
				if((rc.getComparator().equals("<"))&&(!cs.isRNDoverOPEStrippedOff())) cs.getTable().getDBClient().removeRNDLayer(cs, "OPE");
				if((rc.getComparator().equals(">"))&&(!cs.isRNDoverOPEStrippedOff())) cs.getTable().getDBClient().removeRNDLayer(cs, "OPE");
			
				// aus Plain-RCs Cipher-RCs machen
				if(rc.getComparator().equals("=")) {
					rc.setColumnName(cs.getCDETname());
					if(cs.getType() == ColumnType.STRING) rc.setByteTerm(cs.getDETScheme().encrypt(rc.getStringTerm().getBytes()));
					if(cs.getType() == ColumnType.INTEGER) rc.setByteTerm(cs.getDETScheme().encrypt(Misc.longToBytes(rc.getLongTerm())));
					if(cs.getType() == ColumnType.BYTE) rc.setByteTerm(cs.getDETScheme().encrypt(rc.getByteTerm()));					
					rc.setType(ColumnType.BYTE);
				}
				
				if(rc.getComparator().equals(" CONTAINS ")) {
					rc.setColumnName(cs.getCDETname());
					if(cs.getType() == ColumnType.STRING_SET) rc.setByteTerm(cs.getDETScheme().encrypt(rc.getStringTerm().getBytes()));		
					if(cs.getType() == ColumnType.INTEGER_SET) rc.setByteTerm(cs.getDETScheme().encrypt(Misc.longToBytes(rc.getLongTerm())));	
					if(cs.getType() == ColumnType.BYTE_SET) rc.setByteTerm(cs.getDETScheme().encrypt(rc.getByteTerm()));
					rc.setType(ColumnType.BYTE);
				}
				
				if((rc.getComparator().equals(">"))||(rc.getComparator().equals("<"))) {
					rc.setColumnName(cs.getCOPEname());
					
					// compose DBLocation for OPE encryption
					ArrayList<String> tmpColumns = new ArrayList<String>();
					tmpColumns.add(cs.getPlainName());
					DBLocation tmpLocation = new DBLocation(cs.getTable().getKeyspace(), cs.getTable(), null, tmpColumns);
					
					//if(cs.getType() == ColumnType.STRING) rc.setByteTerm(cs.getOPEScheme().encryptString(rc.getStringTerm(), tmpLocation));
					if(cs.getType() == ColumnType.INTEGER) rc.setByteTerm(Misc.longToBytes(cs.getOPEScheme().encrypt(rc.getLongTerm(), tmpLocation)));
					//if(cs.getType() == ColumnType.STRING) rc.setByteTerm(Misc.longToBytes(cs.getOPEScheme().encrypt(Misc.stringToLong(rc.getStringTerm()), tmpLocation)));
					if(cs.getType() == ColumnType.STRING) rc.setByteTerm(Misc.longArrayListToByteArray(cs.getOPEScheme().encryptList(Misc.stringToLongArrayList(rc.getStringTerm()), tmpLocation)));
					//if(cs.getType() == ColumnType.BYTE) rc.setByteTerm(cs.getOPEScheme().encrypt(rc.getByteTerm()));		
					
					
					rc.setType(ColumnType.BYTE);
				}
				
			
			}
			
		}
						
		// involvierte spalten zusammen suchen, die bestehen aus...
		HashSet<ColumnState> involvedSEColumns = new HashSet<ColumnState>();
		HashSet<ColumnState> involvedSelectColumns = new HashSet<ColumnState>();
		
		//...1.: den Spalten in den RowConditions
		for(RowCondition rc: conditions.keySet()) if(rc.getComparator().equals("#")) involvedSEColumns.add(conditions.get(rc));
		
		//...2.: den Spalten aus dem "SELECT" Teil
		for(String c : _columns) {
			for(TableState ts : this.getKeyspaceByName(_keyspace).getTableByPlainName(_table)) {
				if(ts.getColumnByPlainName(c) != null) involvedSelectColumns.add(ts.getColumnByPlainName(c));
			}
		}
		
		HashSet<ColumnState> allInvolvedColumns = new HashSet<ColumnState>();
		allInvolvedColumns.addAll(involvedSEColumns);
		allInvolvedColumns.addAll(involvedSelectColumns);
		
		// pro beteiligter DB ein Request basteln
		HashMap<DBClient, Request> readRequests = new HashMap<DBClient, Request>();
		
		for(ColumnState cs : allInvolvedColumns) {
			
			DBClient db = cs.getTable().getDBClient();
						
			// es gibt noch kein readRequest für diese DB, dann muss einer erstellt werden
			if(!readRequests.containsKey(db)) readRequests.put(db, new Request(RequestType.READ, new DBLocation(cs.getTable().getKeyspace(), cs.getTable(), new ArrayList<RowCondition>(), new ArrayList<String>())));
				
			// neue column an readRequest anhängen
			if(!cs.isEncrypted()) readRequests.get(db).getId().addColumn(cs.getPlainName());
			else{
				// only DET or RND columns are required for decryption, SE columns required for SE, if necessary
				if(cs.getType() == ColumnType.STRING) {
					readRequests.get(db).getId().addColumn(cs.getCDETname());
					readRequests.get(db).getId().addColumn(cs.getCOPEname());  // needed in HBase
					if(involvedSEColumns.contains(cs)) readRequests.get(db).getId().addColumn(cs.getCSEname());
				}
				if(cs.getType() == ColumnType.STRING_SET) {
					readRequests.get(db).getId().addColumn(cs.getCDETname());
					if(involvedSEColumns.contains(cs)) readRequests.get(db).getId().addColumn(cs.getCSEname());
				}
				if(cs.getType() == ColumnType.INTEGER) {
					readRequests.get(db).getId().addColumn(cs.getCOPEname());
					readRequests.get(db).getId().addColumn(cs.getCDETname());
				}
				if(cs.getType() == ColumnType.INTEGER_SET) {
					readRequests.get(db).getId().addColumn(cs.getCDETname());
				}
				if(cs.getType() == ColumnType.BYTE) {
					readRequests.get(db).getId().addColumn(cs.getCDETname());
				}
				if(cs.getType() == ColumnType.BYTE_SET) {
					readRequests.get(db).getId().addColumn(cs.getCDETname());
				}
			}				
			
		}
		
		// ReadRequests sollten jetzt komplett sein bis auf die RowConditions, die werden jetzt angefügt:		
		for(RowCondition rc : conditions.keySet()) {
			readRequests.get(conditions.get(rc).getTable().getDBClient()).getId().addRowCondition(rc); 
		}
		
		// Requests processen
		DecryptedResults results = new DecryptedResults(involvedSelectColumns);
		
		ArrayList<DBClient> DBtasks = new ArrayList<DBClient>();
		
		ExecutorService executor = Executors.newCachedThreadPool();
		List<Future<Result>> encryptedFutureResults = null;
		ArrayList<Result> encryptedResults = new ArrayList<Result>();
		
		// query databases
		Timer t_query = new Timer();
		
				
		try {
			// prepare threads
			for(DBClient db : readRequests.keySet()) {
				db.setCurrentRequest(readRequests.get(db));
				DBtasks.add(db);
			}
			// perform queries
			t_query.start();
			encryptedFutureResults = executor.invokeAll(DBtasks);	
			
			// wait for database answers
			for(Future<Result> futureResult : encryptedFutureResults) encryptedResults.add(futureResult.get());
			t_query.stop();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}							
						
		// decrypt results		
		for(Result result : encryptedResults) results.addResult(result);
			
		t.stop();
		if(!silent) Misc.printStatus("Read query executed in: " + t.getRuntimeAsString() + ", " + results.getSize() + " row(s) retrieved");
		if(!silent) Misc.printStatus("database communication: " + t_query.getRuntimeAsString());
		if(!silent) Misc.printStatus("decryption time:        " + Timer.getTimeAsString(results.getDecryptionTime()));
		if(!silent) Misc.printStatus("protocol overhead:      " + Timer.getTimeAsString(t.getRuntime() - results.getDecryptionTime() - t_query.getRuntime()));
		return results;	
	}
	
	
	
	/**
	 * drops all physical tables belonging to the given table
	 * @param keyspaceName the keyspace that houses the table to be droped
	 * @param tableName
	 */
	public void dropTable(String keyspaceName, String tableName) {
		
		TableState[] tables = null;
		
		// get all physical tables belonging to this logical table
		KeyspaceState keyspace = this.getKeyspaceByName(keyspaceName);
		if(keyspace != null) tables = keyspace.getTableByPlainName(tableName);
		else {
			if(!silent) Misc.printStatus("Couldn't drop table \"" + keyspaceName + "." + tableName + "\", because the keyspace doesn't exist!");
			return;
		}
		
		// drop each
		for(TableState ts: tables) {
			
			// remove from database
			Request dropRequest = new Request(RequestType.DROP_TABLE, new DBLocation(ts.getKeyspace(), ts, null, null));
			ts.getDBClient().processRequest(dropRequest);
						
			// remove from metadata
			keyspace.getAvailableTables().remove(ts);		
		}
		
		if(!silent) Misc.printStatus("Dropped table \"" + keyspaceName + "." + tableName + "\"");
		
	}
	
	
	
	/**
	 * Drops the keyspace (if exists)(and all its tables) with the given name
	 * @param keyspaceName the keyspace to be dropped
	 */
	public void dropKeyspace(String keyspaceName) {
		
		KeyspaceState keyspace = this.getKeyspaceByName(keyspaceName);
		
		if(keyspace != null) {
			
			// remove from all databases
			Request dropRequest = new Request(RequestType.DROP_KEYSPACE, new DBLocation(keyspace, null, null, null));
			for(DBClient db : keyspace.getAvailableDatabases()) db.processRequest(dropRequest);
		
			// remove from metadata
			keyspaces.remove(keyspace);
			
			if(!silent) Misc.printStatus("Dropped keyspace \"" + keyspaceName + "\"");
		}
		else if(!silent) Misc.printStatus("Keyspace \"" + keyspaceName + "\" wasn't dropped, because it didn't exist");
	}
	
	
	/**
	 * cleans up, saves the current state back to the config files
	 */
	public void close() {		
		
		// save all keyspaces to the configuration file
		Document doc = new Document();
		
		Element configurationRoot = new Element("configuration");
		
		Element configurationKeyspaces = new Element("keyspaces");
		for (KeyspaceState ks : keyspaces) configurationKeyspaces.addContent(ks.getThisAsXMLElement());
		configurationRoot.addContent(configurationKeyspaces);
		
		doc.setRootElement(configurationRoot);
		
		XMLOutputter xmlOutput = new XMLOutputter();
		xmlOutput.setFormat(Format.getPrettyFormat());
		try {
			xmlOutput.output(doc, new FileWriter(configFilePath));
			if(!silent) Misc.printStatus("Configuration successfully saved to: \"" + configFilePath + "\"");
		} catch (IOException e) {
			if(!silent) Misc.printStatus("Configuration Saving failed");
		}
		
		// save all keystores
		for(KeyspaceState ks : keyspaces) {
			ks.getKeystore().saveKeystore();
		}
		
		// save all OPE dictionaries
		for(KeyspaceState ks : keyspaces) {
			
			long wordcount = 0;
			
			for(TableState ts : ks.getAvailableTables()) {
				for(ColumnState cs : ts.getAvailableColumns()) {
					
					
					
					if(cs.isEncrypted()) 
						if(cs.getOPEScheme() != null)cs.getOPEScheme().close();
						if(cs.getSEScheme() != null) {
							wordcount += cs.getSEScheme().getWordcount();
							cs.getSEScheme().close();
						}
				}
			}
			
			if(!silent) Misc.printStatus(wordcount + " words encrypted in keyspace \"" + ks.getPlainName() + "\".");
		}
		
		// close all database connections
		DBClientFactory.closeAllConnections();
		
	}

}
