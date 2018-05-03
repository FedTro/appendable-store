package task.store.testobjects;

import java.io.Serializable;

public class ClippedText implements Serializable {

	private String key;
	private String statement;
	private int size;

	public ClippedText(String key, String statement) {
		this.key = key;
		this.statement = statement;
		this.size = statement.length();
	}

	public String getKey() {
		return key;
	}

	public String getStatement() {
		return statement;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	@Override
	public String toString() {
		return "ClippedText [key=" + key + ", statement=" + statement
				+ ", size=" + size + "]";
	}
	
}
