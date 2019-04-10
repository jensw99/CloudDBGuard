package papamanthou;

public class C2 {
	public byte[] c2;
	public byte[] initVector;
	
	C2(byte[] c2, byte[] initVector)
	{
		super();
		this.c2 = c2;
		this.initVector = initVector;
	}
	
	byte[][] toByte()
	{
		byte[][] array = new byte[2][];
		array[0] = this.c2;
		array[1] = this.initVector;
		return array;
	}

}
