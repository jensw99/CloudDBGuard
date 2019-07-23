package papamanthou;

import java.io.IOException;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerByteArray;

public class SerializerForByteArray implements Serializer<ByteArray> {

	private Serializer<byte[]> internalSerializer;
	
	public SerializerForByteArray()
	{
		super();
		internalSerializer = new SerializerByteArray();
		
	}
	
	@Override
	public int compare(ByteArray o1, ByteArray o2) {
		return internalSerializer.compare(o1.toByte(), o2.toByte());
	}

	@Override
	public ByteArray deserialize(DataInput2 arg0, int arg1) throws IOException {
		return new ByteArray(internalSerializer.deserialize(arg0, arg1));
	}

	@Override
	public void serialize(DataOutput2 arg0, ByteArray arg1) throws IOException {
		internalSerializer.serialize(arg0, arg1.toByte());

	}

}
