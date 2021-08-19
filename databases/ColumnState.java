package databases;

import interfaces.SaveableInXMLElement;

import java.math.BigInteger;
import java.util.ArrayList;

import org.jdom2.Element;

import misc.Misc;
import crypto.DETScheme;
import crypto.DET_AES;
import crypto.KeyStoreManager;
import crypto.OPEScheme;
import crypto.OPE_Boldyreva_mOPE;
import crypto.OPE_KS;
import crypto.OPE_RSS;
import crypto.OPE_RowKeyRSS;
import crypto.PRG;
import crypto.RNDScheme;
import crypto.RND_AES;
import crypto.SEScheme;
import crypto.SE_SUISE;
import crypto.SE_SWP2;
import enums.TableProfile;
import enums.ColumnType;



/**
 * Class representing the state of a single plaintext column, including all corresponding ciphertext columns
 *
 * @author Tim Waage
 */
public class ColumnState implements SaveableInXMLElement {

	// tells, if the RND layer above the DET layer was already stripped off
	private boolean RNDoverDETStrippedOff;
	
	// tells, if the RND layer above the OPE layer was already stripped off
	private boolean RNDoverOPEStrippedOff;
	
	// tells if this column is a rowkey column
	private boolean rowkeyColumn;
	
	// tells if the column was already adjusted
	private boolean adjusted;
	
	// tells if this column has to be stored onion encrypted (=true) or not (=false)
	private boolean encrypted;
	
	// the column key
	private byte[] key; 
	
	// the column's name in plaintext
	private String pname;
	
	// the column type
	private ColumnType type;
	
	// the column's names of the encrypted columns
	private String cRNDname;
	private String cDETname;
	private String cOPEname;
	private String cSEname;
		
	// the scheme used for random encryption in this column
	private RNDScheme rnd;
	
	// the scheme used for deterministic encryption in this column
	private DETScheme det;
	
	// the scheme used for order preserving encryption in this column
	private OPEScheme ope;
		
	// the scheme used for searchable encryption in this column
	private SEScheme se;
		
	// the table this columns is associated to
	private TableState table;
	
	// the keystore, this tables schemes are using
	private KeyStoreManager keystore;
	
	
	
	/**
	 * Returns the type of the plaintext column, that this ColumnState object is representing
	 * @return the type of the plaintext column
	 */
	public ColumnType getType() {
		
		return type;
	}
	
	
	
	/**
	 * Constructor, defines what schemes to use, depending on the chosen profile
	 * @param plainName the plaintext name of this column
	 * @param type the data type the column is representing (String, Integer, etc...)
	 * @param profile (the profile, the DB Client runs on)
	 * @param _db database where this column is stored
	 */
	public ColumnState(String plainName, ColumnType _type, TableProfile profile, boolean _rowkey, boolean _encrypted) {
		
		PRG g = new PRG();
		
		key = g.generateRandomPrintableBytes(32); 	// AES key for RND encryption
		
		// TODO: put that key into the keystore
		
		pname = plainName;
		type = _type;
		
		cRNDname = "RND_" + pname;
		cDETname = "DET_" + pname;
		cOPEname = "OPE_" + pname;
		cSEname = "SE_" + pname;
		
		rowkeyColumn = _rowkey;
		encrypted = _encrypted;
		
		adjusted = false;
	} 
	
	
	
	/**
	 * set the columns properties according to the requirements of the table, the encryption schemes in particular
	 * @param ts the table this column belongs to
	 */
	public void adjustColumnToTable(TableState ts) {
		
		if(adjusted) return;
		
		table = ts;
		keystore = table.getKeyspace().getKeystore();
		
		ArrayList<String> tmpColumn = new ArrayList<String>();
		tmpColumn.add(pname);
		
		DBLocation thisColumnsLocation = new DBLocation(table.getKeyspace(), table, null, tmpColumn);
		
		TableProfile profile = table.getProfile();
			
		switch(type) {
				
			case STRING:
				if(profile == TableProfile.FAST) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_KS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = new SE_SUISE(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, thisColumnsLocation);
					//se = new SE_Papa("PAPA", table.getKeyspace().getKeystore(), ts.getDBClient(), thisColumnsLocation, true);
					//se = new SE_SWP2(table.getKeyspace().getKeystore(), ts.getDBClient());       
				}
				else if(profile == TableProfile.ALLROUND) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_RSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = new SE_SUISE(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, thisColumnsLocation);
				}
				else if(profile == TableProfile.STORAGEEFFICIENT) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_Boldyreva_mOPE(table.getKeyspace().getKeystore(), ts.getDBClient(), 27, 27, BigInteger.valueOf(40000000));
					se = new SE_SWP2(table.getKeyspace().getKeystore(), ts.getDBClient());
				}
				else if(profile == TableProfile.OPTIMIZED_READING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_RSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = new SE_SUISE(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, thisColumnsLocation);
				}
				else if(profile == TableProfile.OPTIMIZED_WRITING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_KS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63); 
					//else ope = new OPE_RSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = new SE_SWP2(table.getKeyspace().getKeystore(), ts.getDBClient());
				}
			break;
			
			case STRING_SET:
				if(profile == TableProfile.FAST) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = null;
					ope = null;
					se = new SE_SUISE(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, thisColumnsLocation);
				}
				else if(profile == TableProfile.ALLROUND) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = null;
					ope = null;
					se = new SE_SUISE(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, thisColumnsLocation);
				}
				else if(profile == TableProfile.STORAGEEFFICIENT) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = new SE_SWP2(table.getKeyspace().getKeystore(), ts.getDBClient());
				}
				else if(profile == TableProfile.OPTIMIZED_READING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = new SE_SUISE(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, thisColumnsLocation);
				}
				else if(profile == TableProfile.OPTIMIZED_WRITING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = new SE_SWP2(table.getKeyspace().getKeystore(), ts.getDBClient());
				}
			break;
			
			case INTEGER:
				if(profile == TableProfile.FAST) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_KS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = null; // Integers do not need to be searched on
				}
				else if(profile == TableProfile.ALLROUND) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_RSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = null; // Integers do not need to be searched on
				}
				else if(profile == TableProfile.STORAGEEFFICIENT) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_Boldyreva_mOPE(table.getKeyspace().getKeystore(), ts.getDBClient(), 27, 27, BigInteger.valueOf(40000000));
					se = null; // Integers do not need to be searched on
				}
				else if(profile == TableProfile.OPTIMIZED_READING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_RSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = null; // Integers do not need to be searched on
				}
				else if(profile == TableProfile.OPTIMIZED_WRITING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					if(rowkeyColumn) ope = new OPE_RowKeyRSS(table.getKeyspace().getKeystore(), ts.getDBClient(), 32, 64);
					else ope = new OPE_KS(table.getKeyspace().getKeystore(), ts.getDBClient(), 31, 63);
					se = null; // Integers do not need to be searched on
				}
				break;
				
			case INTEGER_SET:
				if(profile == TableProfile.FAST) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = null;
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.ALLROUND) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = null;
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.STORAGEEFFICIENT) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.OPTIMIZED_READING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.OPTIMIZED_WRITING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = null;
				}
			break;
			
			case BYTE:
				if(profile == TableProfile.FAST) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null; // Byte Blobs don't get OPEed
					se = null; // Byte Blobs do not need to be searched on
				}
				else if(profile == TableProfile.ALLROUND) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null; // Byte Blobs don't get OPEed
					se = null; // Byte Blobs do not need to be searched on
				}
				else if(profile == TableProfile.STORAGEEFFICIENT) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null; // Byte Blobs don't get OPEed
					se = null; // Byte Blobs do not need to be searched on
				}
				else if(profile == TableProfile.OPTIMIZED_READING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null; // Byte Blobs don't get OPEed
					se = null; // Byte Blobs do not need to be searched on
				}
				else if(profile == TableProfile.OPTIMIZED_WRITING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null; // Byte Blobs don't get OPEed
					se = null; // Byte Blobs do not need to be searched on
				}
				break;
		
			case BYTE_SET:
				if(profile == TableProfile.FAST) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = null;
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.ALLROUND) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = null;
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.STORAGEEFFICIENT) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.OPTIMIZED_READING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = null;
				}
				else if(profile == TableProfile.OPTIMIZED_WRITING) {
					
					rnd = new RND_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), key);
					det = new DET_AES(table.getKeyspace().getKeystore(), ts.getDBClient(), table.getIV(), table.getKey());
					ope = null;
					se = null;
				}
			break;
				
		default:
			break;
		} 
		
		adjusted = true;
	}
	
	
	
	/**
	 * Constructor
	 * @param data LinkedHashMap containing all information to initialize the table state
	 */
	public ColumnState(Element data) {
		
		initializeFromXMLElement(data);
	}


	
	/** 
	 * Returns the scheme used for random encryption of this column's content
	 * @return the scheme used for random encryption of this column's content
	 */
	public RNDScheme getRNDScheme() {
		
		return rnd;
	}
	
	
	
	/**
	 * Returns the "encrypted" name of this columns randomly encrypted pendant in the database
	 * @return the "encrypted" name of this columns randomly encrypted pendant in the database
	 */
	public String getCRNDname() {
		
		return cRNDname;
	}
	
	
	
	/**
	 * Returns the "encrypted" name of this columns deterministic pendant in the database
	 * @return the "encrypted" name of this columns deterministic pendant in the database
	 */
	public String getCDETname() {
		
		return cDETname;
	}


	
	/** 
	 * Returns the scheme used for deterministic encryption of this column's content
	 * @return the scheme used for deterministic encryption of this column's content
	 */
	public DETScheme getDETScheme() {
		
		return det;
	}
	
	
	
	/**
	 * Returns the "encrypted" name of this columns order preserving pendant in the database
	 * @return the "encrypted" name of this columns order preserving pendant in the database
	 */
	public String getCOPEname() {
		
		return cOPEname;
	}
	
	
	
	/**
	 * Sets the name of the OPE column manually, required for the SUISE index
	 * @param _opeName the name of the OPE column
	 */
	public void setCOPEname(String _opeName) {
		
		cOPEname = _opeName;
	}


	
	/** 
	 * Returns the scheme used for order-preseving encryption of this column's content
	 * @return the scheme used for order-preseving encryption of this column's content
	 */
	public OPEScheme getOPEScheme() {
		
		return ope;
	}
	
	
	
	/**
	 * Returns the "encrypted" name of this columns searchable pendant in the database
	 * @return the "encrypted" name of this columns searchable pendant in the database
	 */
	public String getCSEname() {
		
		return cSEname;
	}
	
	
	
	/** 
	 * Returns the scheme used for searchable encryption of this column's content
	 * @return the scheme used for searchable encryption of this column's content
	 */
	public SEScheme getSEScheme() {
		
		return se;
	}
	
	
	/**
	 * tells if this column is the row key column of a table
	 * @return true, if this columns is a rowkey column of a table
	 */
	public boolean isRowkeyColumn() {
		
		return rowkeyColumn;
	}
	
	
	/**
	 * tells, if this column is stored onion layer encrypted in the database or not
	 * @return true if encryption is enabled for this column, false otherwise
	 */
	public boolean isEncrypted() {
		
		return encrypted;
	}
	
	
	
	/**
	 * returns the table this column belongs to
	 * @return the table this column belongs to
	 */
	public TableState getTable() {
		
		return table;
	}
	
	
	
	/**
	 * Returns this column's plaintext name
	 * @return this column's plaintext name
	 */
	public String getPlainName() {
		
		return pname;
	}
	
	
	/**
	 * Returns the key necessary for encryptiong the RND layer
	 * @return the key necessary for encryptiong the RND layer
	 */
	public byte[] getKey() {
		
		return key;
	}
	
	
	/**
	 * returns true, if the RND layer was already stripped off the DET layer, false otherwise
	 * @return true, if the RND layer was already stripped off the DET layer, false otherwise
	 */
	public boolean isRNDoverDETStrippedOff() {
		
		if(this.isRowkeyColumn()) return true;  // rowkey columns never have a RND layer
		else return RNDoverDETStrippedOff;
	}
	
	
	
	/**
	 * Sets the information whether the RND layer was removed from the DET layer or not
	 * This should usually only occur with "true", because it makes no sense to put the RND layer back on
	 * @param value true, if the RND layer was removed, false otherwise
	 */
	public void setRNDoverDETStrippedOff(boolean value) {
		
		RNDoverDETStrippedOff = value;
	}
	
	
	
	/**
	 * returns true, if the RND layer was already stripped off the OPE layer, false otherwise
	 * @return true, if the RND layer was already stripped off the OPE layer, false otherwise
	 */
	public boolean isRNDoverOPEStrippedOff() {
		
		if(this.isRowkeyColumn()) return true;  // rowkey columns never have a RND layer
		else return RNDoverOPEStrippedOff;
	}
	
	
	
	/**
	 * Sets the information whether the RND layer was removed from the DET layer or not
	 * This should usually only occur with "true", because it makes no sense to put the RND layer back on
	 * @param value true, if the RND layer was removed, false otherwise
	 */
	public void setRNDoverOPEStrippedOff(boolean value) {
		
		RNDoverOPEStrippedOff = value;
	}
	
	
	
	/**
	 * Returns the column state as XML Element
	 * @return the column state as HashMap object
	 */
	@Override
	public Element getThisAsXMLElement() {
		
		Element columnRoot = new Element("column");
			
		// RND over DET/OPE stripped off
		Element columnRNDoverDETStrippedOffElement = new Element("RNDoverDETStrippedOff");		
		columnRNDoverDETStrippedOffElement.addContent(String.valueOf(RNDoverDETStrippedOff));
		Element columnRNDoverOPEStrippedOffElement = new Element("RNDoverOPEStrippedOff");		
		columnRNDoverOPEStrippedOffElement.addContent(String.valueOf(RNDoverOPEStrippedOff));
		columnRoot.addContent(columnRNDoverDETStrippedOffElement);
		columnRoot.addContent(columnRNDoverOPEStrippedOffElement);
		
		// rowkey
		Element columnRowkey = new Element("rowkey");		
		columnRowkey.addContent(String.valueOf(rowkeyColumn));
		columnRoot.addContent(columnRowkey);
		
		// key
		Element columnKey = new Element("key");
		columnKey.addContent(Misc.ByteArrayToCharString(key));
		columnRoot.addContent(columnKey);
		
		// key
		Element columnEncrypted = new Element("encrypted");
		columnEncrypted.addContent(String.valueOf(encrypted));
		columnRoot.addContent(columnEncrypted);
		
		// plain name
		Element columnPlainName = new Element("plainname");
		columnPlainName.addContent(pname);
		columnRoot.addContent(columnPlainName);
		
		// column type
		Element columnType = new Element("type");
		columnType.addContent(String.valueOf(type));
		columnRoot.addContent(columnType);
		
		// names of the encrypted columns
		Element columnEncryptedColumns = new Element("encryptedColumns");
		
		Element columnCRNDName = new Element("cRNDname");
		columnCRNDName.addContent(cRNDname);
				
		Element columnCDETName = new Element("cDETname");
		columnCDETName.addContent(cDETname);
				
		Element columnCOPEName = new Element("cOPEname");
		columnCOPEName.addContent(cOPEname);		
		
		Element columnCSEName = new Element("cSEname");
		columnCSEName.addContent(cSEname);
		
		columnEncryptedColumns.addContent(columnCRNDName);
		columnEncryptedColumns.addContent(columnCDETName);
		columnEncryptedColumns.addContent(columnCOPEName);
		columnEncryptedColumns.addContent(columnCSEName);
		
		columnRoot.addContent(columnEncryptedColumns);
		
		return columnRoot;
	}


	@Override
	public void initializeFromXMLElement(Element data) {
		
		RNDoverDETStrippedOff = Boolean.valueOf(data.getChild("RNDoverDETStrippedOff").getText());
		RNDoverOPEStrippedOff = Boolean.valueOf(data.getChild("RNDoverOPEStrippedOff").getText());
		
		rowkeyColumn = Boolean.valueOf(data.getChild("rowkey").getText());
		
		key = Misc.CharStringToByteArray(data.getChild("key").getText());
		
		encrypted = Boolean.valueOf(data.getChild("encrypted").getText());
		
		pname = data.getChild("plainname").getText();
		
		type = ColumnType.valueOf(data.getChild("type").getText());
		
		cRNDname = data.getChild("encryptedColumns").getChild("cRNDname").getText();
		cDETname = data.getChild("encryptedColumns").getChild("cDETname").getText();
		cOPEname = data.getChild("encryptedColumns").getChild("cOPEname").getText();
		cSEname = data.getChild("encryptedColumns").getChild("cSEname").getText();
	}

}
