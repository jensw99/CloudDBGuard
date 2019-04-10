package papamanthou;

import java.io.IOException;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerByteArray;

public class SerializerServerValueNode implements Serializer<ServerValueNode> {

	private SerializerByteArray internalByte;
	private SerializerC2 internalC2;

	public SerializerServerValueNode() {
		super();
		internalByte = new SerializerByteArray();
		internalC2 = new SerializerC2();
	}

	@Override
	public int compare(ServerValueNode o1, ServerValueNode o2) {
		int res = internalByte.compare(o1.c1, o2.c1);
		if (res != 0) {
			return res;
		} 
		return internalC2.compare(o1.c2, o2.c2);
	}

	@Override
	public ServerValueNode deserialize(DataInput2 arg0, int arg1) throws IOException {
		return new ServerValueNode(internalByte.deserialize(arg0, arg1),internalC2.deserialize(arg0, arg1));
	}

	@Override
	public void serialize(DataOutput2 arg0, ServerValueNode arg1) throws IOException {
		internalByte.serialize(arg0, arg1.c1);
		internalC2.serialize(arg0, arg1.c2);
	}

}
