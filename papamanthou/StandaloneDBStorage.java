/**
 * 
 */
package papamanthou;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.Drop;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.utils.UUIDs;

import databases.*;

//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Row;
//import com.datastax.driver.core.Session;


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
public class StandaloneDBStorage implements Storage {

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#clearLevelsAndAdd(int, java.util.List)
	 */
	
	private static final String KEYSPACENAME = "papamanthou";
	private static final String MAINTABLENAME = "main";
	private static final String[] TMPSTORAGE = {"tmp1", "tmp2"};
	private static int tmpread = 0;
	private static final String COLUMNHKEYNAME = "hkey";
	private static final String COLUMNLEVELNAME = "level";
	private static final String COLUMNLEVELPOSNAME = "levelpos";
	private static final String COLUMNC1NAME = "c1";
	private static final String COLUMNC2NAME = "c2";
	private static final String COLUMNIVNAME = "iv";
	private static final String COLUMNINDEXNAME = "ind";

	
	private Cluster cluster;
	private Session session;
	private PreparedStatement preparedMainInsert;
	private PreparedStatement[] preparedTmpInsert = new PreparedStatement[2];
	
	private int chunkLevel;	// we read until this level
	private int chunkSize;	// the chunksize we read
	private int currentLevel = 0; // the current level we read in
	private int currentPosInLevel = 0; // the element in the current level we read
	private int currentInsertPos = 0;	// the position we need to continue inserting when inserting several chunks to the same level
	
	private int tmpStorageSize[] = {0, 0};
	
	
	public void insertNodes(int level, List<ServerNode> input){
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
		if(currentInsertPos >= Math.pow(2, level)) currentInsertPos = 0; //if we have filled the level, start at the beginning
	}
	
	public void clearLevels(int untilLevel){
		List<Integer> levels = new ArrayList<>();
		for(int i = 0; i <= untilLevel; i++) levels.add(i);
		
		Statement del = QueryBuilder.delete().from(KEYSPACENAME, MAINTABLENAME)
				.where(QueryBuilder.in(COLUMNLEVELNAME, levels));
		
//		String delete = "Delete from "+ KEYSPACENAME + "." + MAINTABLENAME +
//				" where " + COLUMNLEVELNAME + " IN "+ levels;
		session.execute(del);
	}
	
	@Override
	public void clearLevelsAndAdd(int untilLevel, List<ServerNode> input) {
		clearLevels(untilLevel);
		insertNodes(untilLevel, input);	
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
//		for (int i = 0; i < TMPSTORAGE.length; i++){
//			Truncate truncate = QueryBuilder.truncate(KEYSPACENAME, TMPSTORAGE[i]);
//			session.execute(truncate);
//		}
		tmpStorageSize = new int[]{0, 0};
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getSizeOfTempStorage()
	 */
	@Override
	public int getSizeOfTempStorage() {
//		return tmpStorageSize[tmpread];
		throw new UnsupportedOperationException("Not implemented yet.");
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
		Statement select = QueryBuilder.select(COLUMNC2NAME, COLUMNIVNAME)
				.from(KEYSPACENAME, TMPSTORAGE[tmpread])
				.where(QueryBuilder.in(COLUMNINDEXNAME, values));
		
		ResultSet result = session.execute(select);
		
		for (Row row: result){
			ByteBuffer c2 = row.getBytes(0);
			ByteBuffer iv = row.getBytes(1);
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

//		Statement drop = SchemaBuilder.dropTable(KEYSPACENAME, TMPSTORAGE[tmpread]);
//		session.execute(drop);
//		
//		Statement create = SchemaBuilder.createTable(KEYSPACENAME, TMPSTORAGE[tmpread])
//				.addColumn(COLUMNC2NAME, DataType.blob())
//				.addColumn(COLUMNIVNAME, DataType.blob())
//				.addPartitionKey(COLUMNINDEXNAME, DataType.bigint())
//				.ifNotExists();
//		session.execute(create);
				
//		Statement truncate = QueryBuilder.truncate(KEYSPACENAME, TMPSTORAGE[tmpread]);
//		session.execute(truncate);
	
		tmpStorageSize[tmpread] = 0;
		tmpread = (tmpread + 1) % 2;
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getAllC2UntilLevel(int)
	 */
	@Override
	public List<C2> getAllC2UntilLevel(int untilLevel) {
		
		List<Integer> levels = new ArrayList<>();
		for(int i = 0; i <= untilLevel; i++) levels.add(i);
				
		List<C2> res = new ArrayList<C2>();
		
		Statement select = QueryBuilder.select(COLUMNC2NAME, COLUMNIVNAME)
				.from(KEYSPACENAME, MAINTABLENAME)
				.where(QueryBuilder.in(COLUMNLEVELNAME, levels));
//		String query = "Select " + COLUMNC2NAME + ", " + COLUMNIVNAME  + " from " + KEYSPACENAME + "." + MAINTABLENAME + 
//				" where " + COLUMNLEVELNAME + " IN " + levels;
		ResultSet results = session.execute(select);
		for (Row row: results){
			ByteBuffer c2 = row.getBytes(0);
			ByteBuffer iv = row.getBytes(1);
			C2 newC2=new C2(c2.array(), iv.array());
			res.add(newC2);
		}
		return res;
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
			Select select = QueryBuilder.select(COLUMNC2NAME, COLUMNIVNAME)
					.from(KEYSPACENAME, MAINTABLENAME);
			Statement where = select.where(QueryBuilder.eq(COLUMNLEVELNAME, currentLevel))  //load from current level
					.and(QueryBuilder.gte(COLUMNLEVELPOSNAME, currentPosInLevel))			//everything greater than starting pos
					.and(QueryBuilder.lt(COLUMNLEVELPOSNAME, currentPosInLevel+(chunkSize-chunkElementCounter)));			//and everything less than pos+size
			for(Row r: session.execute(where)){
				chunkElementCounter++;
				currentPosInLevel++;
				res.add(new C2(r.getBytes(0).array(), r.getBytes(1).array())); // DB
			}
			
			if(currentPosInLevel >= Math.pow(2, currentLevel) - 1 || currentPosInLevel == 0){ //level is exhausted or was empty
				currentLevel++;
				currentPosInLevel = 0;
			}
		}
		if(res.isEmpty()) return null;
		return res;
/*		
		
		//current level: could be started, then currentPos != 0 (this should only happen if this level actually has entries)
		//				 or could be too large to fit in chunk 
		//				(or both)
		// then we need to load it alone due to cql
		
		if(currentPosInLevel != 0 || Math.pow(2, currentLevel) - currentPosInLevel > chunkSize){
			Where where = select.where(QueryBuilder.eq(COLUMNLEVELNAME, currentLevel))  //load from current level
					.and(QueryBuilder.gte(COLUMNLEVELPOSNAME, currentPosInLevel))			//everything greater than starting pos
					.and(QueryBuilder.lt(COLUMNLEVELPOSNAME, currentPosInLevel+chunkSize));			//and everything less than pos+size
			for(Row r: session.execute(where)){
				chunkElementCounter++;
				currentPosInLevel++;
				res.add(new C2(r.getBytes(0).array(), r.getBytes(1).array())); // DB
			}
		}
		
		int remainingSpace = chunkSize - chunkElementCounter;
		if(remainingSpace == 0) 
			return res;
		
		//if we came here, there is space in the chunk and we need to go to the next level
		
		
		int numberOfLevels = getNumberOfLevels(); //DB 
		
		
		
		
		List<Integer> levels = new ArrayList<>();
		
		// minimum number of levels to load to remain smaller than chunkSize
		int level = currentLevel, sum = 0;
		while(sum + Math.pow(2, level) <= chunkSize && level <= numberOfLevels){
			sum += Math.pow(2, level);
			levels.add(level);
			level++;
		}
		//load these levels from DB and count number of results while parsing
		Where where = select.where(QueryBuilder.in(COLUMNLEVELNAME, levels)); // 1 DB anfrage
		ResultSet result = session.execute(where);
		for(Row row : result){
			ByteBuffer c = row.getBytes(0);
			ByteBuffer iv = row.getBytes(1);
			res.add(new C2(c.array(),iv.array()));
			chunkElementCounter++;
		}
		
		//now fill remaining chunk incrementally
		//if we have actually loaded all levels we can return this chunk, also if its not full
		if(level == numberOfLevels) return res;
		
		//check if the level has more entries than we want to load in the chunk.
		
*/	
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
		Statement select = QueryBuilder.select().countAll().from(KEYSPACENAME, MAINTABLENAME);
//		String query = "Select count(*) from " + KEYSPACENAME + "." + MAINTABLENAME;
		ResultSet res = session.execute(select);
		long r = res.one().getLong(0);	
		return (int) r;
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
		Statement select = QueryBuilder.select().countAll()
				.from(KEYSPACENAME, MAINTABLENAME)
				.where(QueryBuilder.eq(COLUMNLEVELNAME, level));
//		String query = "select count(*) from " + KEYSPACENAME + "." + MAINTABLENAME +
//				" where " + COLUMNLEVELNAME + "=" + level;
		ResultSet results = session.execute(select);
		return results.one().getLong(0)==0;
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
	
		ByteBuffer hkey = ByteBuffer.wrap(node.hkey);
		ServerValueNode svnode = node.node;
		ByteBuffer c1 = ByteBuffer.wrap(svnode.c1);
		ByteBuffer c2 = ByteBuffer.wrap(svnode.c2.c2);
		ByteBuffer iv = ByteBuffer.wrap(svnode.c2.initVector);
		BoundStatement statement = preparedMainInsert.bind((long) 0, hkey, c1, c2, iv, (long) 0);
		session.execute(statement);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#isKeyInLevel(byte[], int)
	 */
	@Override
	public boolean isKeyInLevel(byte[] hkey, int level) {
		Statement select = QueryBuilder.select().countAll()
				.from(KEYSPACENAME, MAINTABLENAME).allowFiltering()
				.where(QueryBuilder.eq(COLUMNLEVELNAME, level))
				.and(QueryBuilder.eq(COLUMNHKEYNAME, ByteBuffer.wrap(hkey)));
//		String query = "Select Count(*) from " + KEYSPACENAME + "." + MAINTABLENAME +
//				" where " + COLUMNHKEYNAME + "=" + bytesToCQLHexString(hkey) + " and " + COLUMNLEVELNAME + "=" + level;
		ResultSet res = session.execute(select);
		return res.one().getLong(0)==1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#getC1(byte[], int)
	 */
	@Override
	public byte[] getC1(byte[] hkey, int level) {
		//use prepared statements?
		Statement select = QueryBuilder.select(COLUMNC1NAME)
				.from(KEYSPACENAME, MAINTABLENAME).allowFiltering()
				.where(QueryBuilder.eq(COLUMNHKEYNAME, ByteBuffer.wrap(hkey)))
				.and(QueryBuilder.eq(COLUMNLEVELNAME, level));
//		String query = "Select " + COLUMNC1NAME + " from " + KEYSPACENAME + "." + MAINTABLENAME + 
//				" where " + COLUMNHKEYNAME + "=" + bytesToCQLHexString(hkey) + " and " + COLUMNLEVELNAME + "=" + level;
		ResultSet res = session.execute(select);
		return res.one().getBytes(0).array();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see sse.Storage#setup(int)
	 */
	@Override
	public void setup(int levels) {
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect();
		
		session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACENAME +";");
		
		session.execute("CREATE KEYSPACE IF NOT EXISTS "+ KEYSPACENAME + " WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' };");
		
		Create create = SchemaBuilder.createTable(KEYSPACENAME, MAINTABLENAME)
				.addColumn(COLUMNHKEYNAME, DataType.blob())
				.addPartitionKey(COLUMNLEVELNAME, DataType.bigint())			
				.addColumn(COLUMNC1NAME, DataType.blob())
				.addColumn(COLUMNC2NAME, DataType.blob())
				.addColumn(COLUMNIVNAME, DataType.blob())
				.addClusteringColumn(COLUMNLEVELPOSNAME, DataType.bigint())
				.ifNotExists();
		session.execute(create);
		
		Insert insert = QueryBuilder.insertInto(KEYSPACENAME, MAINTABLENAME)
				.value(COLUMNLEVELNAME, QueryBuilder.bindMarker())
				.value(COLUMNHKEYNAME, QueryBuilder.bindMarker())
				.value(COLUMNC1NAME, QueryBuilder.bindMarker())
				.value(COLUMNC2NAME, QueryBuilder.bindMarker())
				.value(COLUMNIVNAME, QueryBuilder.bindMarker())
				.value(COLUMNLEVELPOSNAME, QueryBuilder.bindMarker());
		
		preparedMainInsert = session.prepare(insert);
		
		for(int i = 0; i < 2; i++){
			create = SchemaBuilder.createTable(KEYSPACENAME, TMPSTORAGE[i])
					.addColumn(COLUMNC2NAME, DataType.blob())
					.addColumn(COLUMNIVNAME, DataType.blob())
					.addPartitionKey(COLUMNINDEXNAME, DataType.bigint())
					.ifNotExists();
			session.execute(create);
			
			insert = QueryBuilder.insertInto(KEYSPACENAME, TMPSTORAGE[i])
					.value(COLUMNINDEXNAME, QueryBuilder.bindMarker())
					.value(COLUMNC2NAME, QueryBuilder.bindMarker())
					.value(COLUMNIVNAME, QueryBuilder.bindMarker());
			preparedTmpInsert[i] = session.prepare(insert);
		}
				
	}
	
	public void close(){
		if (session != null){
			session.close();
		}
		if (cluster != null){
			cluster.close();
		}
		
	}

	@Override
	public void deleteMainStorage() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<ServerNode> getNodes(int level) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void deleteMetaData(){
		
	}
	
}
