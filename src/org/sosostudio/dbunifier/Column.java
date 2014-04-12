package org.sosostudio.dbunifier;

public class Column {

	public final static String TYPE_STRING = "string";

	public final static String TYPE_NUMBER = "number";

	public final static String TYPE_DATETIME = "datetime";

	public final static String TYPE_CLOB = "clob";

	public final static String TYPE_BLOB = "blob";

	private String name;

	private String type;

	private int size;

	private boolean nullable;

	private boolean isPrimaryKey;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public boolean getNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public boolean getIsPrimaryKey() {
		return isPrimaryKey;
	}

	public void setIsPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\t[name = " + name + "]\n");
		sb.append("\t[type = " + type + "]\n");
		sb.append("\t[size = " + size + "]\n");
		sb.append("\t[nullable = " + nullable + "]\n");
		sb.append("\t[isPrimaryKey = " + isPrimaryKey + "]\n");
		return sb.toString();
	}

}
