/**
 * 
 */
package papamanthou;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import misc.Misc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import crypto.Index;
import databases.DBClient;
import databases.DBClientCassandra;
import databases.DBLocation;
import enums.DatabaseType;


/**
 * @author daniel Diese Klasse soll die Anbindung an die Datenbank enthalten.
 *         Dazu muss das Interface Storage implementiert werden Die
 *         Dokumentation des erwarteten Verhaltens der einzelnen Methoden findet
 *         sich direkt in Storage.java Die Klasse wird vermutlich eine
 *         Haupttabelle mit den folgenden 6(?) Spalten: 
 *         byte[] hkey 
 *         int level
 *         byte[] c1 
 *         byte[] c2 
 *         byte[] initVector 
 *         anlegen um darin die ServerNode
 *         zu speichern. Die Byte-Arrays haben (momentan, das hängt von der
 *         Krypto-Config ab) eine Länge von 32 Byte. Ein Primärschlüssel für die
 *         Tabelle sind zusammen (hkey, level). hkey alleine genügt leider nicht
 *         Auf dieser Tabelle operieren fast alle Befehle.
 * 
 *         Die Ausnahme sind die Befehle mit Temp im Namen. Diese werden für das
 *         sortieren der Datenbank benötigt und operieren auf einer extra
 *         Tabelle. Diese Tabelle wird immer nur zeitweise benutzt und danach
 *         wieder geleert. Die Tabelle hat zwei Spalten: 
 *         byte[] c2 
 *         byte[] initVector 
 *         Die Länge der Byte-Arrays ist wie oben jeweils 32 Byte. C2
 *         kann als Primärschlüssel verwendet werden. Vermutlich ein bisschenta
 *         schwierig zu implementieren ist das Schreib-/Leseverhalten dieses
 *         temporären Speichers Solange switchTempStorage() nicht aufgerufen
 *         wurde, wird der Lesebefehl (getFromTempStorage) weiter auf den
 *         unveränderten Daten ausgeführt. Erst danach wird zu den neuen Daten
 *         gewechselt. Eventuell lässt sich dieses Verhalten durch eine weitere
 *         Spalte in der Hilfstabelle erzeugen.
 * 
 */
public class DBStorage extends Index implements Storage {

	private static final String[] TMPSTORAGE = {"tmp1", "tmp2"};
	private static int tmpread = 0;
	private static final String COLUMNHKEYNAME = "hkey";
	private static final String COLUMNLEVELNAME = "level";
	private static final String COLUMNLEVELPOSNAME = "levelpos";
	private static final String COLUMNC1NAME = "c1";
	private static final String COLUMNC2NAME = "c2";
	private static final String COLUMNIVNAME = "iv";
	private static final String COLUMNINDEXNAME = "ind";

	
	private CqlSession session;
	
	private PreparedStatement preparedMainInsert;
	private PreparedStatement[] preparedTmpInsert = new PreparedStatement[2];
	
	private int chunkLevel;	// we read until this level
	private int chunkSize;	// the chunksize we read
	private int currentLevel = 0; // the current level we read in
	private int currentPosInLevel = 0; // the element in the current level we read
	private int currentInsertPos = 0;	// the position we need to continue inserting when inserting several chunks to the same level	
	private int tmpStorageSize[] = {0, 0};
	
	
	private HashSet<Integer> validLevels;
	private MemoryHashMapStorage ramStorage = new MemoryHashMapStorage();
	private int lastRamLevelIndex;
	private String mainTableName;
	private String keyspaceName;
	private String configFile;
	
	/**
	 * 
	 * @param _db the DBClient from the upper layer. Used to get IP of the cassandra cluster
	 * @param ramLevel specifies how many levels of the index are held in RAM. This will be 2^(ramlevel)-1 entries
	 * @throws Exception 
	 */
	public DBStorage(DBClient _db, DBLocation location, int ramLevel) throws Exception {
		super(_db);
		if (_db.getType() != DatabaseType.CASSANDRA) throw new Exception("SE_Papa is only implemented with Cassandra");
		session = ((DBClientCassandra) db).getSession();
		ramStorage.setup(ramLevel);
		
		this.lastRamLevelIndex = ramLevel-1;
		
		this.mainTableName = location.getCipherTableName()+"_papaindex_" + location.getColumns().get(0);
		this.keyspaceName = location.getKeyspace().getCipherName();
		this.configFile = "/home/tim/TimDB/" + location.getIdAsPath()+"_levels";
		
		validLevels = loadValidLevels();
	}
	
	@Override
	public void insertNodes(int level, List<ServerNode> input){
		if(input.size() != Math.pow(2, level)){
			System.out.println("Warning: Adding "+input.size()+" elements in level " + level+ ". Level is not full.");
		}
		
		if(level <= lastRamLevelIndex){
			ramStorage.insertNodes(level, input);
		}
		
		else{
			insertNodesToDB(level, input);
		}
		
		validLevels.add(level);
	}
	
	private void insertNodesToDB(int level, List<ServerNode> input){
		for (ServerNode node: input)
		{
			ByteBuffer hkey = ByteBuffer.wrap(node.hkey);
			ServerValueNode svnode = node.node;
			ByteBuffer c1 = ByteBuffer.wrap(svnode.c1);
			ByteBuffer c2 = ByteBuffer.wrap(svnode.c2.c2);
			ByteBuffer iv = ByteBuffer.wrap(svnode.c2.initVector);
			BoundStatement statement = preparedMainInsert.bind((long) level, hkey, c1, c2, iv,(long) currentInsertPos++);
			session.execute(statement);
		}
		currentInsertPos = 0; 
		//now start at the beginning. This should always be the case because it should input.size() == levelsize
	}
	
	@Override
	public List<ServerNode> getNodes(int level){
		List<ServerNode> nodes = new ArrayList<>();
		
		if(level <= lastRamLevelIndex){
			nodes = ramStorage.getNodes(level);
		}
		else{
			nodes = getNodesFromDB(level);
		}
		return nodes;
	}
	
	private List<ServerNode> getNodesFromDB(int level){
		List<ServerNode> nodes = new ArrayList<>();
		
		if( !validLevels.contains(level))
			return nodes;
		
		Relation rel = Relation.column(COLUMNLEVELNAME).isEqualTo(literal(level));
		SimpleStatement select = selectFrom(keyspaceName, mainTableName)
				.columns(COLUMNHKEYNAME, COLUMNC1NAME, COLUMNC2NAME, COLUMNIVNAME).where(rel).allowFiltering().build();
		ResultSet res = session.execute(select);
		for(Row r : res){
			ByteBuffer hkey = r.getByteBuffer(COLUMNHKEYNAME);
			ByteBuffer c1 = r.getByteBuffer(COLUMNC1NAME);
			ByteBuffer c2 = r.getByteBuffer(COLUMNC2NAME);
			ByteBuffer iv = r.getByteBuffer(COLUMNIVNAME);
			ServerValueNode val = new ServerValueNode(c1.array(), c2.array(), iv.array());
			ServerNode node = new ServerNode(hkey.array(), val);
			nodes.add(node);
		}
		return nodes;
	}
	
	@Override
	public void clearLevels(int untilLevel){
		//we just note that these levels are now emtpy, but we wont delete them physically, thats too expensive
		for(int i = 0; i <= untilLevel; i++){
			validLevels.remove(i);
		}
		
		//this clears EXCLUDING the untilLevel !
		if(untilLevel > lastRamLevelIndex){
			ramStorage.clearLevels(lastRamLevelIndex+1);
		}
		else{
			ramStorage.clearLevels(untilLevel);
		}
		
				
		
	}
	
	@Override
	public void clearLevelsAndAdd(int untilLevel, List<ServerNode> input) {
		clearLevels(untilLevel);
		insertNodes(untilLevel, input);
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getAllC2UntilLevel(int)
	 */
	@Override
	public List<C2> getAllC2UntilLevel(int untilLevel) {
		
		List<C2> res = new ArrayList<C2>();
		
		if(untilLevel <= lastRamLevelIndex){
			res = ramStorage.getAllC2UntilLevel(untilLevel);
		}
		
		//get all from ramStorage and rest from DB
		else { /*untilLevel > ramlevel*/
			res = ramStorage.getAllC2UntilLevel(lastRamLevelIndex);
			
			List<Integer> levels = new ArrayList<>();
			for(int i = lastRamLevelIndex+1; i <= untilLevel; i++) {
				if( validLevels.contains(i))
					levels.add(i);
			}
			
			List<Term> levelTerms= new ArrayList<Term>();
			for(int level : levels) levelTerms.add(literal(level));
			
			Relation rel = Relation.column(COLUMNLEVELNAME).in(tuple(levelTerms));
			SimpleStatement select = selectFrom(keyspaceName, mainTableName)
					.columns(COLUMNC2NAME, COLUMNIVNAME).where(rel).allowFiltering().build();
	
			ResultSet results = session.execute(select);
			for (Row row: results){
				ByteBuffer c2 = row.getByteBuffer(0);
				ByteBuffer iv = row.getByteBuffer(1);
				C2 newC2=new C2(c2.array(), iv.array());
				res.add(newC2);
			}
		}
		
		//debug
		//System.out.println(res.size());
		
		return res;
	}

	

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getFirstEmptyLevel()
	 */
	@Override
	public int getFirstEmptyLevel() {
		int level = 0;
		while(!isLevelEmpty(level)) level++;
		return level;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getNumberOfEntries()
	 */
	@Override
	public int getNumberOfEntries() {
		// add up the valid Levels we know.
		int entries = 0;
		for(Integer i: validLevels){
			entries += 1 << i;
		}
		return entries;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getNumberOfLevels()
	 */
	@Override
	public int getNumberOfLevels() {
		int v = getNumberOfEntries();
		if (v == 0){
			return 1;
		}
		return (int) (Math.log(v)/Math.log(2)+1);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#isLevelEmpty(int)
	 */
	@Override
	public boolean isLevelEmpty(int level) {
		return ! validLevels.contains(level);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#insertInLevel0(sse.ServerNode)
	 */
	@Override
	public void insertInLevel0(ServerNode node) {
		if (!isLevelEmpty(0)){
			System.out.println("Level 0 is not empty, can not insert here");
			return;
		}
		ramStorage.insertInLevel0(node);
		
		validLevels.add(0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#isKeyInLevel(byte[], int)
	 */
	@Override
	public boolean isKeyInLevel(byte[] hkey, int level) {
		
		if(! validLevels.contains(level))
			return false;
		
		if(level <= lastRamLevelIndex){
			return ramStorage.isKeyInLevel(hkey, level);
		}
		else{
			
			Relation rel1 = Relation.column(COLUMNLEVELNAME).isEqualTo(literal(level));
			Relation rel2 = Relation.column(COLUMNHKEYNAME).isEqualTo(literal(ByteBuffer.wrap(hkey)));
			SimpleStatement select = selectFrom(keyspaceName, mainTableName).countAll()
					.where(rel1, rel2).allowFiltering().build();
	
			ResultSet res = session.execute(select);
			
			//debug
			//boolean ret = res.one().getLong(0)==1;
			//System.out.println(ret + " hkey: " + Misc.ByteArrayToString(hkey) + " level: " + level);			
			//return ret;
			
			return res.one().getLong(0)==1;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getC1(byte[], int)
	 */
	@Override
	public byte[] getC1(byte[] hkey, int level) {
		
		if(! validLevels.contains(level))
			return null;

		if(level <= lastRamLevelIndex){
			return ramStorage.getC1(hkey, level);
		}
		else{
			Relation rel1 = Relation.column(COLUMNLEVELNAME).isEqualTo(literal(level));
			Relation rel2 = Relation.column(COLUMNHKEYNAME).isEqualTo(literal(ByteBuffer.wrap(hkey)));
			SimpleStatement select = selectFrom(keyspaceName, mainTableName)
					.columns(COLUMNC1NAME).where(rel1, rel2).allowFiltering().build();
	
			ResultSet res = session.execute(select);
			
			//debug
			//byte[] ret = res.one().getBytes(0).array();
			//System.out.println(Misc.ByteArrayToString(ret));
			
			//return ret;
			return res.one().getByteBuffer(0).array();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#setup(int)
	 */
	@Override
	public void setup(int levels) {
	
		session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspaceName+" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
		
		SimpleStatement create = SchemaBuilder.createTable(keyspaceName, mainTableName).ifNotExists()
				.withPartitionKey(COLUMNLEVELNAME, DataTypes.BIGINT)
				.withColumn(COLUMNHKEYNAME, DataTypes.BLOB)		
				.withColumn(COLUMNC1NAME, DataTypes.BLOB)
				.withColumn(COLUMNC2NAME, DataTypes.BLOB)
				.withColumn(COLUMNIVNAME, DataTypes.BLOB)
				.withClusteringColumn(COLUMNLEVELPOSNAME, DataTypes.BIGINT).build();
		session.execute(create);
		
		SimpleStatement insert = insertInto(keyspaceName, mainTableName)
				.value(COLUMNLEVELNAME, bindMarker())
				.value(COLUMNHKEYNAME, bindMarker())
				.value(COLUMNC1NAME, bindMarker())
				.value(COLUMNC2NAME, bindMarker())
				.value(COLUMNIVNAME, bindMarker())
				.value(COLUMNLEVELPOSNAME, bindMarker()).build();
		
		preparedMainInsert = session.prepare(insert);
		
		for(int i = 0; i < 2; i++){
			create = SchemaBuilder.createTable(keyspaceName, TMPSTORAGE[i])
					.ifNotExists()
					.withPartitionKey(COLUMNINDEXNAME, DataTypes.BIGINT)
					.withColumn(COLUMNC2NAME, DataTypes.BLOB)
					.withColumn(COLUMNIVNAME, DataTypes.BLOB).build();
			session.execute(create);
			
			insert = insertInto(keyspaceName, TMPSTORAGE[i])
					.value(COLUMNINDEXNAME, bindMarker())
					.value(COLUMNC2NAME, bindMarker())
					.value(COLUMNIVNAME, bindMarker()).build();
			preparedTmpInsert[i] = session.prepare(insert);
		}
				
		// now try to read valid levels to RAM
		// if we didn't restore validLevels, this will result in nothing.
		initRamFromDB(validLevels);
		
		
	}
	
	private void writeRamToDB(Set<Integer> validLevels){
		for(int level = 0; level <= lastRamLevelIndex; level++){
			if(validLevels.contains(level)){
				List<ServerNode> input = ramStorage.getNodes(level);
				insertNodesToDB(level, input);
			}
		}
	}
	
	private void initRamFromDB(Set<Integer> validLevels){
		for(int level = 0; level <= lastRamLevelIndex; level++){
			if(validLevels.contains(level)){
				ramStorage.insertNodes(level, getNodesFromDB(level));
			}
		}
	}
	
	public void close(){
		//See if the ram changed compared to what we stored to disk
		HashSet<Integer> old = loadValidLevels();
		if (! old.equals(validLevels)){
			writeRamToDB(validLevels);
			saveValidLevels();
		}
	}

	public void deleteConfigurationFiles(){
		File toDelete = new File(configFile);
		toDelete.delete();
	}
	
	@SuppressWarnings("unchecked")
	private HashSet<Integer> loadValidLevels(){
		try {
			FileInputStream fis = new FileInputStream(configFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			HashSet<Integer> toReturn = (HashSet<Integer>) ois.readObject();
			ois.close();
			fis.close();
			return toReturn;
		} catch (IOException | ClassNotFoundException | NullPointerException e) {
			return new HashSet<>();
		}
		
	}
	
	private void saveValidLevels() {
		try {
			FileOutputStream fos = new FileOutputStream(configFile);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(validLevels);
			oos.close();
			fos.close();		
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void printValidLevels(){
		for(int i: validLevels){
			System.out.print(i +" ");
		}
		System.out.println();
	}

	@Override
	public void deleteMainStorage() {
		session.execute("DROP TABLE IF EXISTS " + keyspaceName + "." + mainTableName + ";");
	}
	
	public CqlSession getSession(){
		return session;
	}
	
	@Override
	public void deleteMetaData(){
		deleteConfigurationFiles();
	}
	
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#setupClearLevelsAndAddChunks(int)
	 */
	@Override
	public void setupClearLevelsAndAddChunks(int untilLevel) {
		clearLevels(untilLevel);
		chunkLevel = untilLevel;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#sendChunk(java.util.List)
	 */
	@Override
	public void sendChunk(List<ServerNode> input) {
		insertNodes(chunkLevel, input);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#clearTempStorage()
	 */
	@Override
	public void clearTempStorage() {
		tmpStorageSize = new int[]{0, 0};
		// we dont clear physically, too expensive
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getSizeOfTempStorage()
	 */
	@Override
	public int getSizeOfTempStorage() {
		return tmpStorageSize[tmpread];
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#putToTempStorage(java.util.List, int)
	 */
	@Override
	public void putToTempStorage(List<C2> input, int index) {
		for(C2 c2 : input){
			ByteBuffer c = ByteBuffer.wrap(c2.c2);
			ByteBuffer iv = ByteBuffer.wrap(c2.initVector);			
			BoundStatement statement = preparedTmpInsert[(tmpread+1)%2].bind((long) index++, c, iv);
			session.execute(statement);
			tmpStorageSize[(tmpread+1)%2]++;
		}
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getFromTempStorage(int, int)
	 */
	@Override
	public List<C2> getFromTempStorage(int startindex, int chunkSize) {
		
		List<C2> res = new ArrayList<>(); 
		List<Integer> values = new ArrayList<>();
		
		if(tmpStorageSize[tmpread] == 0) return null;
		if(startindex + chunkSize >= tmpStorageSize[tmpread]) chunkSize = tmpStorageSize[tmpread] - startindex;
		if(chunkSize <= 0) return null;
		
		for(int i = startindex; i < startindex+chunkSize; i++){
			values.add(i);
		}
		
		List<Term> valueTerms= new ArrayList<Term>();
		for(int val : values) valueTerms.add(literal(val));
		
		Relation rel1 = Relation.column(COLUMNINDEXNAME).in(tuple(valueTerms));
		SimpleStatement select = selectFrom(keyspaceName, TMPSTORAGE[tmpread])
				.columns(COLUMNC2NAME, COLUMNIVNAME).where(rel1).allowFiltering().build();
		
		ResultSet result = session.execute(select);
		
		for (Row row: result){
			ByteBuffer c2 = row.getByteBuffer(0);
			ByteBuffer iv = row.getByteBuffer(1);
			C2 newC2 = new C2(c2.array(), iv.array());
			res.add(newC2);
		}
		if(res.size() == 0) return null;
		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#switchTempStorage()
	 */
	@Override
	public void switchTempStorage() {
		tmpStorageSize[tmpread] = 0;
		tmpread = (tmpread + 1) % 2;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#setupChunksGetAllC2UntilLevel(int, int)
	 */
	@Override
	public void setupChunksGetAllC2UntilLevel(int untilLevel, int chunkSize) {
		this.chunkLevel = untilLevel;
		this.chunkSize = chunkSize;
		this.currentLevel = 0;
		this.currentPosInLevel = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getNextChunk()
	 */
	@Override
	public List<C2> getNextChunk() {
		int chunkElementCounter = 0; //count how many results we added to the chunk
		List<C2> res = new ArrayList<>();
	
		//int numberOfLevels = getNumberOfLevels(); // to determine when to stop 
		
		// do it all incrementally:
		while(chunkElementCounter < chunkSize && currentLevel <= chunkLevel){
			
			Relation rel1 = Relation.column(COLUMNLEVELPOSNAME).isGreaterThanOrEqualTo(literal(currentPosInLevel));//everything greater than starting pos
			Relation rel2 = Relation.column(COLUMNLEVELPOSNAME).isLessThan(literal(currentPosInLevel+(chunkSize-chunkElementCounter)));//and everything less than pos+size
			Relation rel3 = Relation.column(COLUMNLEVELNAME).isEqualTo(literal(currentLevel));//load from current level
			SimpleStatement select = selectFrom(keyspaceName, mainTableName)
					.columns(COLUMNC2NAME, COLUMNIVNAME).where(rel1, rel2, rel3).allowFiltering().build();
			
			for(Row r: session.execute(select)){
				chunkElementCounter++;
				currentPosInLevel++;
				res.add(new C2(r.getByteBuffer(0).array(), r.getByteBuffer(1).array())); // DB
			}
			
			if(currentPosInLevel >= Math.pow(2, currentLevel) - 1 || currentPosInLevel == 0){ //level is exhausted or was empty
				currentLevel++;
				currentPosInLevel = 0;
			}
		}
		if(res.isEmpty()) return null;
		return res;

	}
}
