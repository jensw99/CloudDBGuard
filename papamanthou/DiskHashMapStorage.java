package papamanthou;

import java.util.List;
import java.util.Map;

import org.mapdb.*;

public class DiskHashMapStorage extends HashMapStorage {
	private DB db;
	private static int tempCounter = 0;

	public DiskHashMapStorage() {
		super();
		db = DBMaker.tempFileDB().fileMmapEnableIfSupported().closeOnJvmShutdown().executorEnable().make();
	}

	@Override
	protected List<C2> getTempStorage() {
		tempCounter++;
		return db.indexTreeList("temp" + Integer.toString(tempCounter), new SerializerC2()).create();
	}

	@Override
	protected Map<ByteArray, ServerValueNode> getHashMap() {
		return db.hashMap("map" + Integer.toString(HashMaps.size() - 1)).keySerializer(new SerializerForByteArray())
				.valueSerializer(new SerializerServerValueNode()).create();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
