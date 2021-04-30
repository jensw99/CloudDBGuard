package crypto;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Vector;

import org.jdom2.Element;

import misc.FileSystemHelper;
import misc.Misc;
import enums.ColumnType;
import databases.DBClient;
import databases.DBLocation;
import databases.Result;
import databases.RowCondition;



/**
 * Contains the logic required for encrypting and searching using the SUISE scheme
 * @author Tim Waage
 *
 */
public class SE_SUISE extends SEScheme{
	
	//private PseudorandomGenerator g;
	private PRF f;
	
	// security parameter, key length in bit
	private int lambda;
	
	// search history
	private HashSet<byte[]> omega;
	
	// client side location of the search history
	private String searchHistoryPath;
	
	// the SUISE index
	private SE_SUISE_Index index;
	
	// type of the rowkey column, important for storing rowkeys as identifiers in the indexes
	private ColumnType rowkeyColumnType;
	
	// the plain location, that this scheme's instance encrypts
	private DBLocation location;
	
	private byte[] keyK1;
	
	private byte[] keyK2;
	
	
	
	/**
	 * Constructor for the SUISE scheme
	 * @param _lambda security parameter, specifies key lengths of k1 and k2
	 * @param _ks the keystore this scheme will use
	 */
	public SE_SUISE(KeyStoreManager _ks, DBClient _db, int _lambda, DBLocation _location) {
		
		super("SUISE", _ks, _db);
		
		lambda = _lambda;	
		location = _location;
		
		keyK1 = ks.getKeyFor("SUISE_K1", lambda);
		keyK2 = ks.getKeyFor("SUISE_K2", lambda);
		
		searchHistoryPath = "C:/Users/Jens/OneDrive/Uni/Bachelor_Uni_Frankfurt/Bachelorarbeit/Metadata/SE_SUISE_omega";
		
		f = new PRF();
	    index = new SE_SUISE_Index(_db, _location);
		
		loadClientSideSearchHistory();					
		
	}
	
	
	
	/**
	 * loads the search history omega from the client
	 */
	private void loadClientSideSearchHistory() {		
		
		File metaData = new File(searchHistoryPath);
		
		// if there is no metadata from previous encryptions
		// generate and save them
		if(!metaData.exists()) { 
			
			// create empty history set 
			omega = new HashSet<byte[]>();
			// save it
			FileSystemHelper.writeHashSetToFile(omega, searchHistoryPath);
		}
		
		// otherwise load metadata from file
		else { 			
			omega = FileSystemHelper.readHashSetFromFile(searchHistoryPath);
		}
	}
	
	
	
	/**
	 * saves the current search history to the client
	 */
	private void saveClientSideSearchHistory() {
				
		FileSystemHelper.writeHashSetToFile(omega, searchHistoryPath);
		
	}
	
	
	
		
	@Override							
	public String encrypt(String input, DBLocation id) {
				
		//AddToken(K, f, omega)
		SE_SUISE_AddToken alpha_f = addToken(input, id);
				
		//Add(alpha_f, c, c_fett, gamma) 
		add(alpha_f);
		


		//Enc(K, f);
		return f.encryptString_AES_CBC(input, keyK2);
	}
	
		
	
	/**
	 * Generates AddTokens for new files
	 * @param input the new content
	 * @param identifier the document identifier
	 * @return AddToken corresponding to the new input file
	 */
	private SE_SUISE_AddToken addToken(String input, DBLocation identifier) {
		
		HashSet<byte[]> c_quer = new HashSet<byte[]>();
		
		//create f_quer = list of unique (key)words
		Set<String> f_quer = Misc.getUniqueWords(input);
		
		wordcount += f_quer.size();
			
		PRG g = new PRG();
		
		//create empty list x;
		Vector<byte[]> x = new Vector<byte[]>();
		
		
		for(String word: f_quer) {	
			
			byte[] s_i = g.generateRandomBytes(lambda);
			//compute corresponding search token
					
			//byte[] r_w_i = f.encryptCBC(next.getBytes(), k1);
			byte[] r_w_i = PRF.compute_SHA1(word.getBytes(), keyK1);
			
			//if that search token was used before, add it to x
			if(omega.contains(Misc.ByteArrayToString(r_w_i))) x.add(r_w_i);
					
			//set c_i = H_r_w_i(s_i)||s_i
			byte[] H_r_w_i_s_i = PRF.compute_SHA1(s_i, r_w_i);
			byte[] c_i = new byte[H_r_w_i_s_i.length + s_i.length];
		
			System.arraycopy(H_r_w_i_s_i, 0, c_i, 0, H_r_w_i_s_i.length);
			System.arraycopy(s_i, 0, c_i, H_r_w_i_s_i.length, s_i.length);
			
			//ByteBuffer c_iAsByteBuffer = ByteBuffer.wrap(c_i);
			c_quer.add(c_i);
		}
		
		//sort c_quer lexicographically
		//Collections.sort(c_quer, UnsignedBytes.lexicographicalComparator());
		
		//create AddToken object	
		return new SE_SUISE_AddToken(identifier, c_quer, x);
	}
	
	
	
	/**
	 * the add procedure as it appears in the paper
	 * @param alpha_f the token to be added
	 */
	private void add(SE_SUISE_AddToken alpha_f) {
		
			// set gamma_f[inputFileName] = alpha_f.c
									  // keyspace.table.rowkeyColumnName=rowkeyValue.column
			index.insertGammafDataSet(alpha_f.getID(), alpha_f.getCAsByteBuffers());
			
			// update gamma_w
			index.updateGammawDataSet(alpha_f);					
	}
	
	
	
	/**
	 * Performs a search
	 * @param searchword the keyword for which we search
	 * @param id the database path describing where inside the database to search (keyspace.table.column)
	 * @return a set of Strings representing the document identifiers of the matching documents 
	 */
	public SE_RowIdentifierSet search(String searchword, DBLocation id) {
		
		//SearchToken
		byte[] r_w = searchToken(searchword);
				
		//Search(r_w, gamma_w)
		SE_RowIdentifierSet results = suiseSearch(r_w, id);
				
		// save search history on client side
		saveClientSideSearchHistory();
		
		return results;
	}
	
	
	
	/**
	 * Computes a search token 
	 * @param searchword the word this token is representing
	 * @return the search token
	 */
	private byte[] searchToken(String searchword) {
		
		byte[] r_w = PRF.compute_SHA1(searchword.getBytes(), keyK1);
		
		omega.add(r_w);
		
		return r_w;
	}
	
	
	
	/**
	 * performs a search
	 * @param r_w token of the search word
	 * @param id location within the database where to search is performed
	 * @return a set of keys of rows in which the searchword was found
	 */
	private SE_RowIdentifierSet suiseSearch(byte[] r_w, DBLocation id) {
		
		// search result will have the type of the rowkey column of the table
		SE_RowIdentifierSet overallResults = null;
		if(id.getTable().getRowkeyColumn().isEncrypted()) overallResults = new SE_RowIdentifierSet(ColumnType.BYTE);
		else overallResults = new SE_RowIdentifierSet(id.getTable().getRowkeyColumn().getType());
		
		
		// check, if the word has already been searched for...			
		Result r = index.checkForSearchToken(r_w);
		
		boolean wordWasSearchedBefore = false;
		
		// if so, add the results to the final result set
		if(overallResults.getType() == ColumnType.STRING)			
			if((r.getStringSetsFrom("i_w")/*.get(0)*/ != null)
			&&(!r.getStringSetsFrom("i_w").isEmpty())
			&&(r.getStringSetsFrom("i_w").get(0).size() > 0)) {
				overallResults.setStringSet(r.getStringSetsFrom("i_w").get(0));
				wordWasSearchedBefore = true;
			}
				
		if(overallResults.getType() == ColumnType.INTEGER)
			if((r.getIntSetsFrom("i_w")/*.get(0)*/ != null)
			&&(!r.getIntSetsFrom("i_w").isEmpty())
			&&(r.getIntSetsFrom("i_w").get(0).size() > 0)) {
				overallResults.setIntSet(r.getIntSetsFrom("i_w").get(0));
				wordWasSearchedBefore = true;
			}
			
		if(overallResults.getType() == ColumnType.BYTE)
			if((r.getByteSetsFrom("i_w")/*.get(0)*/ != null)
			&&(!r.getByteSetsFrom("i_w").isEmpty())
			&&(r.getByteSetsFrom("i_w").get(0).size() > 0)) {
				overallResults.setByteSet(r.getByteSetsFrom("i_w").get(0));
				wordWasSearchedBefore = true;
			}
		

		if(wordWasSearchedBefore) return overallResults;
		
		// otherwise...
		else {
			
			//create empty List I_w
			SE_RowIdentifierSet I_w = null;
			if(id.getTable().getRowkeyColumn().isEncrypted()) I_w = new SE_RowIdentifierSet(ColumnType.BYTE);
			else I_w = new SE_RowIdentifierSet(id.getTable().getRowkeyColumn().getType());
		
			//do for every c_quer in gamma_f
			Result gamma_fResult = index.getGammaf();			
			
			
			//if rowkeys are of type String
			//if(I_w.getType() == ColumnType.STRING) {
			
			
			HashMap<byte[], Set<byte[]>> gamma_f = gamma_fResult.getKeyByteSetsFrom("c_quer");
			
			
			if(gamma_f != null) // if column exists
			for(byte[] identifier : gamma_f.keySet()) {
		
				Set<byte[]> c_quer = gamma_f.get(identifier);
				
				// iterate through c_i's
				Iterator<byte[]> it = c_quer.iterator();
				while(it.hasNext()) {
			
					byte[] c_i = it.next();
					//System.out.println(Misc.ByteArrayToString(c_i));
					
					byte[] l_i = new byte[20];
					byte[] r_i = new byte[lambda];
				
					// split c_i in l_i and r_i
					System.arraycopy(c_i, 0, l_i, 0, 20);
					System.arraycopy(c_i, 20, r_i, 0, lambda);
			  	
					// check if H_r_w(r_i) = l_i
					if(Arrays.equals(PRF.compute_SHA1(r_i, r_w), l_i)) {
						
						// word found! add ID(f) to I_w 
						if(I_w.getType() == ColumnType.BYTE) if(I_w.getByteSet().add(identifier)) break;
						else if(I_w.getType() == ColumnType.STRING) if(I_w.getStringSet().add(Misc.ByteArrayToCharString(identifier))) break;
						else if(I_w.getType() == ColumnType.INTEGER) if(I_w.getIntSet().add(Misc.bytesToLong(identifier))) break;
						
					}
				}
			}
		
			// save results, even when search was not successful
			if(I_w.getSize() == 0) index.createKeyInGammaw(r_w, I_w.getType());
			
			// save every match
			else {
				if(I_w.getType() == ColumnType.STRING) for(String newIdentifier : I_w.getStringSet()) index.updateGammawDataSet(r_w, newIdentifier, 0, null, ColumnType.STRING);				
				if(I_w.getType() == ColumnType.INTEGER) for(long newIdentifier : I_w.getIntSet()) index.updateGammawDataSet(r_w, null, newIdentifier, null, ColumnType.INTEGER);
				if(I_w.getType() == ColumnType.BYTE) for(byte[] newIdentifier : I_w.getByteSet()) index.updateGammawDataSet(r_w, null, 0, newIdentifier, ColumnType.BYTE);
			}
		
			return I_w;
			
		}		
	}



	@Override
	public HashSet<String> encryptSet(HashSet<String> input, DBLocation id) {
		// TODO Auto-generated method stub
		return null;
	}
	
	

	@Override
	public Element getThisAsXMLElement() {
		
		Element schemeRoot = new Element("se");
		
		Element schemeIdentifier = new Element("identifier");
		schemeIdentifier.addContent(name);
		schemeRoot.addContent(schemeIdentifier);
		
		Element schemeLambda = new Element("lambda");
		schemeLambda.addContent(String.valueOf(lambda));
		schemeRoot.addContent(schemeLambda);
		
		Element schemeSearchHistoryPath = new Element("searchhistorypath");
		schemeSearchHistoryPath.addContent(String.valueOf(searchHistoryPath));
		schemeRoot.addContent(schemeSearchHistoryPath);
		
		return schemeRoot;
	}



	@Override
	public void initializeFromXMLElement(Element data) {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void delete() {
		// TODO Auto-generated method stub
		
	}

}
