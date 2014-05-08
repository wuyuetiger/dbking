package org.sosostudio.dbunifier;

public enum ColumnType {

	TYPE_STRING("String", "String"), TYPE_NUMBER("Number", "BigDecimal"), TYPE_TIMESTAMP(
			"Timestamp", "Timestamp"), TYPE_CLOB("Clob", "char[]"), TYPE_BLOB(
			"Blob", "byte[]"), TYPE_UNKNOWN("", "");

	private final String name;

	private final String type;

	private ColumnType(String name, String type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

}
