package papamanthou;

import java.util.Arrays;

public class ClientNode implements Comparable<ClientNode> {
	public int cnt;
	public int id;
	public Op op;

	public String word;

	public ClientNode(byte[] input) {
		super();
		fromByte(input);
	}

	public ClientNode(String word, int id, Op op, int cnt) {
		this.word = word;
		this.id = id;
		this.op = op;
		this.cnt = cnt;
	}

	@Override
	public int compareTo(ClientNode o) {
		if (!this.word.equals(o.word)) {
			return this.word.compareTo(o.word);
		}
		if (this.id != o.id) {
			if (this.id < o.id) {
				return -1;
			} else {
				return 1;
			}
		}
		return this.op.compareTo(o.op);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == ClientNode.class) {
			return this.compareTo((ClientNode) obj) == 0;
		}
		return false;
	}

	public void fromByte(byte[] input) {
		byte[] wordByte = getSubarray(0, input);
		this.word = new String(wordByte, Utils.utf8);
		byte[] idByte = getSubarray(wordByte.length + 1, input);
		this.id = Utils.byteArrayToInt(idByte);
		byte[] opByte = getSubarray(wordByte.length + idByte.length + 2, input);
		this.op = Op.fromValue(Utils.byteArrayToInt(opByte));
		byte[] cntByte = getSubarray(wordByte.length + idByte.length + opByte.length + 3, input);
		this.cnt = Utils.byteArrayToInt(cntByte);
	}

	private byte[] getSubarray(int start, byte[] input) {
		int pos = start;
		while (pos < input.length) {
			if (input[pos] == Utils.pause) {
				break;
			}
			pos++;
		}
		return Arrays.copyOfRange(input, start, pos);
	}

	public byte[] toByte() {
		byte[] wordByte = word.getBytes(Utils.utf8);
		byte[] idByte = Utils.intToByteArray(id);
		byte[] opByte = Utils.intToByteArray(op.ordinal());
		byte[] cntByte = Utils.intToByteArray(cnt);
		
		return Utils.concat(Utils.concat(wordByte, idByte), Utils.concat(opByte, cntByte));
	}

}
