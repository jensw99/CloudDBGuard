package papamanthou;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Keys implements Serializable{

	private static final long serialVersionUID = -5389756303019024698L;
	
	private byte[] esk;
	private ArrayList<byte[]> k = new ArrayList<>();
	private byte[] initVector;
	private byte[] eskIv;
	
	public Keys(byte[] esk, byte[] initVector)
	{
		this.esk = esk.clone();
		this.initVector = initVector.clone();
	}
	
	public int numberOfKeys(){
		return k.size();
	}
	
	public void addKey(byte[] k){
		this.k.add(k);
	}
	
	public void addKey(byte[] k, int index){
		this.k.set(index, k);
	}

	public byte[] getEsk() {
		return esk;
	}

	public void setEskIv(byte[] eskIv){
		this.eskIv = eskIv;
	}
	
	public byte[] getKey(int index) {
		return k.get(index);
	}

	public byte[] getInitVector() {
		return initVector;
	}

	public byte[] getEskIv() {
		return eskIv;
	}
	
	
}


