package papamanthou;

import java.io.IOException;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerByteArray;

public class SerializerC2 implements Serializer<C2> {
	
	private SerializerByteArray internal;
	
	public SerializerC2()
	{
		super();
		internal = new SerializerByteArray();
	}

	@Override
	public int compare(C2 o1, C2 o2) {
		int res = internal.compare(o1.c2, o2.c2);
		if (res != 0) {
			return res;
		} 		
		return internal.compare(o2.initVector, o2.initVector);
	}

	@Override
	public void serialize(DataOutput2 out, C2 value) throws IOException {
		internal.serialize(out,value.c2);
		internal.serialize(out, value.initVector);
	}

	@Override
	public C2 deserialize(DataInput2 input, int available) throws IOException {
		return new C2(internal.deserialize(input, available), internal.deserialize(input, available));
	}

}
