package papamanthou;

class ServerValueNode {
	public byte[] c1;
	public C2 c2;


	ServerValueNode() {
	}

	ServerValueNode(byte[] c1, byte[] c2, byte[] initVector) {
		this(c1, new C2(c2,initVector));
	}
	
	ServerValueNode(byte[] c1, C2 c2) {
		super();
		this.c1 = c1;
		this.c2 = c2;
	}
	

}
