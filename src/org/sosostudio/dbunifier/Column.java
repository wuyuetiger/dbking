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

	public Column() {
	}

	public Column(String name, String type, int size, boolean nullable,
			boolean isPrimaryKey) {
		this.name = name;
		this.type = type;
		this.size = size;
		this.nullable = nullable;
		this.isPrimaryKey = isPrimaryKey;
	}

	public String getName() {
		return name;
	}

	public Column setName(String name) {
		this.name = name;
		return this;
	}

	public String getType() {
		return type;
	}

	public Column setType(String type) {
		this.type = type;
		return this;
	}

	public int getSize() {
		return size;
	}

	public Column setSize(int size) {
		this.size = size;
		return this;
	}

	public boolean getNullable() {
		return nullable;
	}

	public Column setNullable(boolean nullable) {
		this.nullable = nullable;
		return this;
	}

	public boolean getIsPrimaryKey() {
		return isPrimaryKey;
	}

	public Column setIsPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
		return this;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\t[name = " + name + "]");
		sb.append("[type = " + type + "]");
		sb.append("[size = " + size + "]");
		sb.append("[nullable = " + nullable + "]");
		sb.append("[isPrimaryKey = " + isPrimaryKey + "]\n");
		return sb.toString();
	}

}
