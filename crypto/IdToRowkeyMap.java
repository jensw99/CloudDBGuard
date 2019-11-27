package crypto;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import databases.RowCondition;


/**
 * holds maps for all tables that connect integer mailIds (given by SE schemes)
 * to the respective rowkeys used in the table schema.
 * This is a Singleton to ensure that only one map is present for each table.
 * @author christian
 *
 */
public class IdToRowkeyMap implements Serializable{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6479429264725133359L;
	private static final String savePath = "/home/tim/TimDB/idMap.ser";
	// keeps track if this object has been saved
	private static boolean saved = false;
	
	//The singleton instance
	private static IdToRowkeyMap object;
	
	
	private final Map<String, HashMap<Integer, RowCondition>> map = new HashMap<>();
	
	public static IdToRowkeyMap getInstance(){
		load();
		return object;
	}
		
	private IdToRowkeyMap(){}
		
	public void insertToMap(String tableName, int id, RowCondition rc){
		if(!map.containsKey(tableName)){
			map.put(tableName, new HashMap<>());
		}
		map.get(tableName).put(id, rc);
		saved = false;
	}
	
	public RowCondition getById(String tableName, int id){
		RowCondition result = null;
		if(map == null) return null;
		result = map.get(tableName).get(id);
		return result;
	}
	
	public void deleteMap(String tableName){
		map.remove(tableName);
		saved = false;
	}
	
	public int size(String tableName){
		if(map == null || map.get(tableName) == null) return 0;
		return map.get(tableName).size();
	}

	public void saveMap(){
		if(saved == false)
			save();
		saved = true;
	}
	
	private static void load(){
		try {
			FileInputStream fis = new FileInputStream(new File(savePath));
			ObjectInputStream ois = new ObjectInputStream(fis);
			object = (IdToRowkeyMap) ois.readObject();
			ois.close();
			fis.close();
		} catch (IOException | ClassNotFoundException e) {
			object = new IdToRowkeyMap();
		}
	}
	
	private static void save(){
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(new File(savePath));
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(object);
			oos.close();
			fos.close();
			saved = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
