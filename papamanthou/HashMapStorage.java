package papamanthou;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;

abstract class HashMapStorage implements Storage {
	protected List<Map<ByteArray, ServerValueNode>> HashMaps;
	private List<Iterator<ServerValueNode>> Iterators;
	private List<C2> readTempStorage;
	private List<C2> writeTempStorage;
	private int chunkSize;
	private int chunkLevel;

	abstract protected Map<ByteArray, ServerValueNode> getHashMap();

	abstract protected List<C2> getTempStorage();

	@Override
	public void clearLevels(int untilLevel) {
		for (int i = 0; i < untilLevel; i++) {
			HashMaps.get(i).clear();
		}
	}
	
	public int getSizeOfTempStorage(){
		return readTempStorage.size();
	}
	
	@Override
	public void insertNodes(int level, List<ServerNode> input){
		if(HashMaps.size() == level) HashMaps.add(getHashMap());
		for (ServerNode node : input) {
			HashMaps.get(level).put(new ByteArray(node.hkey), node.node);
		}
	}
	
	@Override
	public List<ServerNode> getNodes(int level){
		List<ServerNode> nodes = new ArrayList<>();
		HashMaps.get(level).entrySet().forEach((Entry<ByteArray, ServerValueNode> e) -> {
			nodes.add(new ServerNode(e.getKey().toByte(), e.getValue()));
		});
		return nodes;
	}
	
	@Override
	public void clearLevelsAndAdd(int untilLevel, List<ServerNode> input) {
		clearLevels(untilLevel);
		insertNodes(untilLevel, input);
	}

	public void setupClearLevelsAndAddChunks(int untilLevel) {
		this.chunkLevel = untilLevel;
		clearLevels(untilLevel);
	}

	public void sendChunk(List<ServerNode> input) {
		for (ServerNode node : input) {	
			HashMaps.get(chunkLevel).put(new ByteArray(node.hkey), node.node);
		}
	}

	@Override
	public List<C2> getAllC2UntilLevel(int level) {
		List<C2> list = new ArrayList<C2>();
		for (int i = 0; i <= level; i++) {
			for (ServerValueNode node : HashMaps.get(i).values()) {
				list.add(node.c2);
			}
		}
		return list;
	}

	public void setupChunksGetAllC2UntilLevel(int level, int chunkSize) {
		this.chunkLevel = level;
		this.chunkSize = chunkSize;
		Iterators = new ArrayList<Iterator<ServerValueNode>>();

		for (Map<ByteArray, ServerValueNode> map : HashMaps.subList(0, level + 1)) {
			Iterators.add(map.values().iterator());
		}

	}

	public List<C2> getNextChunk() {
		int index;
		for (index = 0; index <= chunkLevel && !Iterators.get(index).hasNext(); index++)
			;

		List<C2> list = new ArrayList<C2>();
		while (list.size() < chunkSize) {
			if (index > chunkLevel) {
				break;
			}
			if (!Iterators.get(index).hasNext()) {
				index++;
				continue;
			}
			list.add(Iterators.get(index).next().c2);
		}

		if (list.size() > 0) {
			return list;
		}
		return null;
	}

	@Override
	public boolean isLevelEmpty(int level) {
		return HashMaps.get(level).size() == 0;
	}

	@Override
	public void insertInLevel0(ServerNode node) {
		if (!isLevelEmpty(0)) {
			System.out.println("Can not insert here. Choosen level is full. Will not do anything");
			return;
		}
		HashMaps.get(0).put(new ByteArray(node.hkey), node.node);
	}

	@Override
	public boolean isKeyInLevel(byte[] hkey, int level) {
		return HashMaps.get(level).containsKey(new ByteArray(hkey));
	}

	@Override
	public byte[] getC1(byte[] hkey, int level) {
		return HashMaps.get(level).get(new ByteArray(hkey)).c1;
	}

	@Override
	public void setup(int levels) {
		HashMaps = new ArrayList<Map<ByteArray, ServerValueNode>>(levels);
		for (int i = 0; i < levels; i++) {
			HashMaps.add(getHashMap());
		}
	}

	@Override
	public int getFirstEmptyLevel() {
		int level;
		for (level = 0; level < HashMaps.size(); level++) {
			if (isLevelEmpty(level)) {
				return level;
			}

		}
		return HashMaps.size();
	}

	@Override
	public int getNumberOfEntries() {
		int result = 0;
		for (int i = 0; i < HashMaps.size(); i++) {
			result += HashMaps.get(i).size();
		}
		return result;
	}

	@Override
	public int getNumberOfLevels() {
		return HashMaps.size();
	}

	public void clearTempStorage() {
		if (readTempStorage == null) {
			readTempStorage = getTempStorage();
		} else {
			readTempStorage.clear();
		}
		if (writeTempStorage == null) {
			writeTempStorage = getTempStorage();
		} else {
			writeTempStorage.clear();
		}
		
	}

	public void putToTempStorage(List<C2> input, int index) {
		for (int i = 0; i < input.size(); i++) {
			if (i + index < writeTempStorage.size()) {
				writeTempStorage.set(i + index, input.get(i));
			} else {
				writeTempStorage.add(input.get(i));
			}

		}
	}

	public List<C2> getFromTempStorage(int startindex, int chunkSize) {
		if (startindex >= readTempStorage.size() || chunkSize<=0) {
			return null;
		}
		return readTempStorage.subList(startindex, Math.min(readTempStorage.size(), startindex + chunkSize));
	}
	
	public void switchTempStorage(){
		readTempStorage=writeTempStorage;
		writeTempStorage= getTempStorage();
	}
	
	public void deleteMainStorage(){
		HashMaps = null;
	}
	
	public void deleteMetaData(){
		
	}
}