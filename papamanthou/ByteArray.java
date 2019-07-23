package papamanthou;

import java.io.IOException;
import java.io.Serializable;

class ByteArray implements Serializable, Comparable<ByteArray> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1016164086081817429L;
	private byte[] array;

	private void writeObject(java.io.ObjectOutputStream stream) throws IOException {
		stream.defaultWriteObject();
		stream.writeInt(array.length);
		for (int i = 0; i < array.length; i++)
			stream.writeByte(array[i]);
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.array = new byte[in.readInt()];
		for (int i = 0; i<array.length; i++){
			array[i] = in.readByte();
		}
			
	}
	
	

	ByteArray(byte[] array) {
		this.array = array.clone();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() != ByteArray.class) {
			return false;
		}
		ByteArray other = (ByteArray) obj;

		return this.compareTo(other) == 0;
	}

	@Override
	public int hashCode() {
		if (array == null) {
			return 0;
		}
		int result = 3;
		for (int i = 0; i < array.length; i++) {
			result = 37 * result + array[i];
		}

		return result;

	}

	byte[] toByte() {
		return array.clone();
	}

	@Override
	public int compareTo(ByteArray o) {
		if (o == null)
			return 1;
		if (this.array.length < o.array.length)
		{
			return -1;
		}
		if (this.array.length> o.array.length)
		{
			return 1;
		}
		for (int i = this.array.length-1; i >= 0; i--) {
			if (this.array[i]<o.array[i])
			{
				return -1;
			}
			if (this.array[i] > o.array[i])
			{
				return 1;
			}
		}
		
		return 0;
	}

}