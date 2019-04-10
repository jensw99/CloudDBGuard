package papamanthou;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryHashMapStorage extends HashMapStorage {

	@Override
	protected Map<ByteArray, ServerValueNode> getHashMap() {
		return new HashMap<ByteArray, ServerValueNode>();
	}

	@Override
	protected List<C2> getTempStorage() {
		return new ArrayList<C2>();
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}