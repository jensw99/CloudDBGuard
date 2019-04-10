package papamanthou;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Client {
	
	private static final String serializedKeysPath = "/home/tim/TimDB/papa_keys.ser";
	
	final private static int chunkSize = 100;
	final private static int oSortRecursionWidth = 10;

	private int l;
	private Crypto crypto;
	private Storage storage;
	private boolean simpleRebuildAlgo;

	public Client(boolean simpleRebuildAlgo) {
		super();
		this.simpleRebuildAlgo = simpleRebuildAlgo;
		crypto = new Crypto();
	}

	public void setup(Storage storage) {
		setup(storage, 1);
	}

	public void setup(Storage storage, int n) {
		this.l = (int) Math.ceil(Math.log(n) / Math.log(2)) + 1;
		crypto.generateKeys(l);
		this.storage = storage;
		this.storage.setup(l);
	}

	public void setup(Storage storage, Keys keys) {
		this.l = keys.numberOfKeys();
		crypto.setKeys(keys);
		this.storage = storage;
		this.storage.setup(l);
	}

	private ServerNode encodeEntry(int level, ClientNode node) {
		byte[] token = crypto.prf(crypto.hash(node.word), level);
		String s = "0" + node.op.toString() + node.cnt;
		byte[] hkey = crypto.keyedHash(s, token);

		byte[] b = crypto.keyedHash("1" + node.op.toString() + node.cnt, token);
		byte[] c1 = Utils.xor(Utils.intToByteArray(node.id), b);

		return new ServerNode(hkey, new ServerValueNode(c1, getC2(node)));
	}

	private C2 getC2(String word, int id, Op op, int cnt) {
		ClientNode node = new ClientNode(word, id, op, cnt);
		return getC2(node);
	}

	private C2 getC2(ClientNode node) {
		return new C2(crypto.encrypt(node.toByte()), crypto.getLastEskInitVector());
	}

	public int[] search(String w) {
		byte[][] tks = new byte[l][];

		for (int i = 0; i < l; i++) {
			tks[i] = crypto.prf(crypto.hash(w), i);
		}

		Set<Integer> result = new HashSet<Integer>();
		for (int level = storage.getNumberOfLevels() - 1; level >= 0; level--) {

			for (int cnt = 0;; cnt++) {
				byte[] idByte = lookUp(tks[level], Op.ADD, cnt, level);
				int id = Utils.byteArrayToInt(idByte);
				if (id == Utils.NOTFOUND) {
					break;
				} else {
					result.add(id);
				}
			}

			for (int cnt = 0;; cnt++) {
				byte[] idByte = lookUp(tks[level], Op.DEL, cnt, level);
				int id = Utils.byteArrayToInt(idByte);
				if (id == Utils.NOTFOUND) {
					break;
				} else {
					result.remove(id);
				}
			}
		}

		int[] res = new int[result.size()];
		int index = 0;
		for (Integer entry : result) {
			res[index] = entry;
			index++;
		}
		return res;
	}

	private byte[] lookUp(byte[] token, Op op, int cnt, int level) {
		
		String s = "0" + op.toString() + cnt;
		byte[] hkey = crypto.keyedHash(s, token);
		if (!storage.isKeyInLevel(hkey, level)) {
			return Utils.intToByteArray(Utils.NOTFOUND);
		}

		byte[] c1 = storage.getC1(hkey, level);
		s = "1" + op.toString() + cnt;
		byte[] hash = crypto.keyedHash(s, token);
		return Utils.xor(c1, hash);

	}

	private void rebuild(int level, String word, int id, Op op) {
		if (simpleRebuildAlgo) {
			simpleRebuild(level, word, id, op);
		} else {
			chunkwiseRebuild(level, word, id, op);
		}
	}

	private void simpleRebuild(int level, String word, int id, Op op) {
		List<ClientNode> B = new ArrayList<ClientNode>();
		B.add(new ClientNode(word, id, op, 0));

		for (C2 entry : storage.getAllC2UntilLevel(level - 1)) {
			B.add(new ClientNode(crypto.decrypt(entry.c2, entry.initVector)));
		}
		B.sort(null);

		int i = 0;
		while (i < B.size() - 1) {
			ClientNode current = B.get(i);
			ClientNode next = B.get(i + 1);
			if (current.word == next.word && current.id == next.id && current.op != next.op) {
				B.remove(i);
				B.remove(i);
			}
			i++;
		}

		String lastWord = "";
		int cntAdd = 0;
		int cntDel = 0;

		for (i = 0; i < B.size(); i++) {
			if (!B.get(i).word.equals(lastWord)) {
				cntAdd = 0;
				cntDel = 0;
			}

			if (B.get(i).op == Op.ADD) {
				B.get(i).cnt = cntAdd;
				cntAdd++;
			} else {
				B.get(i).cnt = cntDel;
				cntDel++;
			}

			lastWord = new String(B.get(i).word);
		}

		crypto.newLevelKey(level);

		List<ServerNode> T = new ArrayList<ServerNode>(B.size());
		for (ClientNode node : B) {
			T.add(encodeEntry(level, node));
		}
		T.sort(null);
		storage.clearLevelsAndAdd(level, T);
	}

	private void chunkwiseRebuild(int level, String word, int id, Op op) {
		storage.clearTempStorage();
		List<C2> list = new ArrayList<C2>();

		list.add(getC2(word, id, op, 0));
		storage.putToTempStorage(list, 0);
		int index = 1;

		storage.setupChunksGetAllC2UntilLevel(level - 1, chunkSize);
		List<C2> chunk = storage.getNextChunk();
		while (chunk != null) {
			storage.putToTempStorage(chunk, index);
			index += chunk.size();
			chunk = storage.getNextChunk();
		}

		storage.switchTempStorage();
		oblivousSort(true, level);

		crypto.newLevelKey(level);
		index = 0;
		ClientNode lastElem = null;
		int lastElemPos = 0;
		String lastWord = "";
		int cntAdd = 0;
		int cntDel = 0;
		chunk = storage.getFromTempStorage(index, chunkSize);

		while (chunk != null) {
			List<ClientNode> B = new ArrayList<ClientNode>();
			for (C2 entry : chunk) {
				B.add(new ClientNode(crypto.decrypt(entry.c2, entry.initVector)));
			}

			if (lastElem != null) {
				B.add(0, lastElem);
			}

			int i = 0;
			while (i < B.size() - 1) {
				ClientNode current = B.get(i);
				ClientNode next = B.get(i + 1);
				if (current.word == next.word && current.id == next.id && current.op != next.op) {
					B.remove(i);
					B.remove(i);
				}
				i++;
			}
			
			int lowerLimit = 1;
			if (lastElem== null)
			{
				lowerLimit = 0;
			}
			for (i = lowerLimit; i < B.size(); i++) {
				if (!B.get(i).word.equals(lastWord)) {
					cntAdd = 0;
					cntDel = 0;
				}

				if (B.get(i).op == Op.ADD) {
					B.get(i).cnt = cntAdd;
					cntAdd++;
				} else {
					B.get(i).cnt = cntDel;
					cntDel++;
				}

				lastWord = new String(B.get(i).word);
			}

			lastElem = B.remove(B.size() - 1);
		
			List<C2> T = new ArrayList<C2>(B.size());
			for (ClientNode node : B) {
				T.add(getC2(node));
			}

			if (index == 0) {
				storage.putToTempStorage(T, 0);
				lastElemPos = T.size();
			} else {
				storage.putToTempStorage(T, index - 1);
				lastElemPos = index + T.size() - 1;
			}

			index += chunkSize;
			chunk = storage.getFromTempStorage(index, chunkSize);
		}

		if (lastElem != null) {
			List<C2> T = new ArrayList<C2>(1);
			T.add(getC2(lastElem));
			// System.out.println(Integer.toString(storage.getSizeOfTempStorage())+"
			// , "+ Integer.toString(lastElemPos));
			storage.putToTempStorage(T, lastElemPos);
		}

		storage.switchTempStorage();
		oblivousSort(false, level);

		storage.setupClearLevelsAndAddChunks(level);
		index = 0;
		// System.out.println("Size before rebuild: " +
		// Integer.toString(storage.getSizeOfTempStorage()));
		chunk = storage.getFromTempStorage(index, chunkSize);

		while (chunk != null) {
			List<ServerNode> B = new ArrayList<ServerNode>();
			for (C2 entry : chunk) {
				ClientNode node = new ClientNode(crypto.decrypt(entry.c2, entry.initVector));
				B.add(encodeEntry(level, node));
			}
			storage.sendChunk(B);
			index += chunkSize;
			chunk = storage.getFromTempStorage(index, chunkSize);
		}
		//System.out.println("Size after rebuild: " + Integer.toString(storage.getNumberOfEntries()));

	}

	private void oblivousSort(boolean encryptedSortKey, int level) {
		// System.out.println("oblivousSort: " + Integer.toString(level) + " , "
		// + Integer.toString(storage.getSizeOfTempStorage()));

		int index = 0;
		List<C2> chunk = storage.getFromTempStorage(index, chunkSize);
		List<ClientNode> B = new ArrayList<ClientNode>(chunkSize);
		List<ServerNode> S = new ArrayList<ServerNode>(chunkSize);
		List<C2> T = new ArrayList<C2>(chunkSize);

		while (chunk != null) {
			T.clear();
			if (encryptedSortKey) {
				B.clear();
				for (C2 entry : chunk) {
					B.add(new ClientNode(crypto.decrypt(entry.c2, entry.initVector)));
				}
				B.sort(null);
				for (ClientNode entry : B) {
					T.add(getC2(entry));
				}
			} else {
				S.clear();
				for (C2 entry : chunk) {
					ClientNode node = new ClientNode(crypto.decrypt(entry.c2, entry.initVector));
					S.add(encodeEntry(level, node));
				}
				S.sort(null);
				for (ServerNode node : S) {
					T.add(node.node.c2);
				}
			}

			storage.putToTempStorage(T, index);
			index += chunkSize;
			chunk = storage.getFromTempStorage(index, chunkSize);
		}
		storage.switchTempStorage();
		if (index > chunkSize) {
			oblivousSort(encryptedSortKey, level, 0, (int) Math.pow(2, level));
		}
	}

	private void oblivousSort(boolean encryptedSortKey, int level, int offset, int ende) {
		/*
		 * List<C2>res = storage.getFromTempStorage(0, 2000);
		 * List<ClientNode> ba = new ArrayList<ClientNode>(2000);
		 * for(C2 r: res){
		 * ba.add(new ClientNode(crypto.decrypt(r.c2, r.initVector)));
		 * }
		 * ba.sort(null);
		 * for (ClientNode b: ba){
		 * System.out.println(b.word +" " + b.id +" "+ b.op);
		 * }
		 */
		/*System.out.println("oblivousSort Recursive: " + Integer.toString(level) + " " + Integer.toString(offset) + " "
				+ Integer.toString(ende));
		System.out.println("Storage size before: " + Integer.toString(storage.getSizeOfTempStorage()));
		*/
		int blockSize = chunkSize;
		while (blockSize * oSortRecursionWidth < ende - offset) {
			blockSize *= oSortRecursionWidth;
		}
		if (blockSize > chunkSize * oSortRecursionWidth) {
			for (int off = offset; off < Math.min(offset + blockSize, ende); off += blockSize) {
				oblivousSort(encryptedSortKey, level, off, Math.min(off + blockSize - 1, ende));
			}
		}

		List<List<ClientNode>> list = new ArrayList<List<ClientNode>>(oSortRecursionWidth);
		List<C2> writeBuffer = new ArrayList<C2>();
		int cachingSize = chunkSize / oSortRecursionWidth;
		int emptyList = 0;
		int writePos = offset;

		int[] readLevel = new int[oSortRecursionWidth];
		for (int i = 0; i < oSortRecursionWidth; i++) {
			readLevel[i] = offset + i * blockSize + cachingSize;
		}
		int[] maxReadLevel = new int[oSortRecursionWidth];
		for (int i = 0; i < oSortRecursionWidth; i++) {
			maxReadLevel[i] = Math.min(offset + (i + 1) * blockSize - 1, ende);
		}

		for (int i = 0; i < oSortRecursionWidth; i++) {
			List<C2> temp = storage.getFromTempStorage(i * blockSize, cachingSize);
			if (temp == null) {
				list.add(null);
			} else {
				list.add(new ArrayList<ClientNode>(cachingSize));
				for (C2 entry : temp) {
					list.get(i).add(new ClientNode(crypto.decrypt(entry.c2, entry.initVector)));
				}
			}
		}

		boolean done = false;
		while (!done) {
			while (allListsNotEmptyOrConsumed(list, writeBuffer)) {
				int minIndex = 0;
				if (encryptedSortKey) {
					Map<Integer, ClientNode> firstElems = new HashMap<Integer, ClientNode>();
					for (int i = 0; i < list.size(); i++) {
						if (list.get(i) != null) {
							firstElems.put(i, list.get(i).get(0));
						}
					}
					Map.Entry<Integer, ClientNode> minEntry = null;
					for (Map.Entry<Integer, ClientNode> entry : firstElems.entrySet()) {
						if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) < 0) {
							minEntry = entry;
						}
					}
					minIndex = (int) minEntry.getKey();
					// System.out.println(minEntry.getValue().word);
				} else {
					Map<Integer, ServerNode> firstElems = new HashMap<Integer, ServerNode>();
					for (int i = 0; i < list.size(); i++) {
						if (list.get(i) != null) {
							firstElems.put(i, encodeEntry(level, list.get(i).get(0)));
						}
					}
					Map.Entry<Integer, ServerNode> minEntry = null;
					for (Map.Entry<Integer, ServerNode> entry : firstElems.entrySet()) {
						if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) < 0) {
							minEntry = entry;
						}
					}
					minIndex = (int) minEntry.getKey();
				}
				writeBuffer.add(getC2(list.get(minIndex).remove(0)));
				/**
				 * if (list.get(0) != null) {
				 * System.out.println("list 0 size: " + list.get(0).size());
				 * }
				 * if (list.get(1) != null) {
				 * System.out.println("list 1 size: " + list.get(1).size());
				 * }
				 */
			}
			// System.out.println("OUT");

			int size = writeBuffer.size();
			storage.putToTempStorage(writeBuffer, writePos);
			writeBuffer.clear();
			writePos += size;

			for (int i = 0; i < oSortRecursionWidth; i++) {
				if (list.get(i) == null) {
					continue;
				}
				if (list.get(i).size() == 0) {
					emptyList = i;
				}
			}
			if (readLevel[emptyList] > maxReadLevel[emptyList]) {
				list.set(emptyList, null);
			} else {
				int readSize = Math.min(cachingSize, maxReadLevel[emptyList] - readLevel[emptyList] + 1);
				/**
				 * System.out.println("Read level: " + readLevel[emptyList]);
				 * System.out.println("At pos 100: " +
				 * storage.getFromTempStorage(100, 1).get(0).c2[0]);
				 */
				List<C2> temp = storage.getFromTempStorage(readLevel[emptyList], readSize);
				readLevel[emptyList] += readSize;
				if (temp != null) {
					list.set(emptyList, new ArrayList<ClientNode>(cachingSize));
					for (C2 entry : temp) {
						list.get(emptyList).add(new ClientNode(crypto.decrypt(entry.c2, entry.initVector)));
					}
				} else {
					list.set(emptyList, null);
				}
			}
			if (Collections.frequency(list, null) == oSortRecursionWidth) {
				done = true;
			}
		}
		storage.switchTempStorage();
		//System.out.println("Storage size after: " + Integer.toString(storage.getSizeOfTempStorage()));

		List<C2> res = storage.getFromTempStorage(0, 2000);
		List<ClientNode> ba = new ArrayList<ClientNode>(2000);
		for (C2 r : res) {
			ba.add(new ClientNode(crypto.decrypt(r.c2, r.initVector)));
		}
		// ba.sort(null);
		/*
		for (ClientNode b : ba) {
			System.out.println(b.word + " " + b.id + " " + b.op);
		}
		System.out.println("ENDE");
	*/
	}

	private <T> boolean allListsNotEmptyOrConsumed(List<List<T>> list, List<C2> writeBuffer) {
		if (writeBuffer.size() >= chunkSize) {
			return false;
		}

		List<List<T>> notNullLists = new ArrayList<List<T>>();
		list.forEach(l -> {
			if (l != null)
				notNullLists.add(l);
		});
		if (notNullLists.size() == 0) {
			return false;
		}
		for (List<T> l : notNullLists) {
			if (l.size() == 0) {
				return false;
			}
		}
		return true;

	}

	private void update(String[] w, int id, Op op) {
		for (String word : w) {

			int level = storage.getFirstEmptyLevel();

			if (level == 0) {
				crypto.newLevelKey(0);
				ClientNode node = new ClientNode(word, id, op, 0);
				ServerNode compNode = encodeEntry(0, node);
				storage.insertInLevel0(compNode);

			} else {
				this.l = Math.max(l, level+1);
				rebuild(level, word, id, op);
			}
		}
	}

	public Keys getKeys() {
		return crypto.getKeys();
	}

	public byte[] getEsk() {
		return getKeys().getEsk();
	}

	public byte[] getInitVector() {
		return getKeys().getInitVector();
	}

	public byte[][] getK() {
		byte[][] k = new byte[getKeys().numberOfKeys()][];
		for(int i = 0; i < k.length; i++)
			k[i] = getKeys().getKey(i);
		return k;
	}

	public void remove(String[] w, int id) {
		update(w, id, Op.DEL);
	}

	public void add(String[] w, int id) {
		update(w, id, Op.ADD);
	}

	public int getNumberOfElements() {
		return storage.getNumberOfEntries();
	}

	public void close() {
		storage.close();
	}
}