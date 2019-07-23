package papamanthou;

class ServerNode implements Comparable<ServerNode> {
	byte[] hkey;
	ServerValueNode node;

	ServerNode() {
	}

	ServerNode(byte[] hkey, ServerValueNode node) {
		this.hkey = hkey;
		this.node = node;
	}

	@Override
	public int compareTo(ServerNode o) {
		if (o == null) {
			return 1;
		}
		if (this.hkey == null) {
			return -1;
		}
		if (o.hkey == null) {
			return 1;
		}

		int thisLast = Utils.last(this.hkey);
		int oLast = Utils.last(o.hkey);
		if (thisLast < oLast) {
			return -1;
		}
		if (thisLast > oLast) {
			return 1;
		}
		if (thisLast == -1) {
			return 0;
		}
		for (int i = thisLast; i >= 0; i--) {
			if (this.hkey[i] < o.hkey[i]) {
				return -1;
			}
			if (this.hkey[i] > o.hkey[i]) {
				return 1;
			}
		}
		return 0;
	}

}
