package papamanthou;

public enum Op {
	ADD, DEL;

	public static Op fromValue(int value) {
		return Op.values()[value];
	}

}
