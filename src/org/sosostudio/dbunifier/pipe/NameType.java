package org.sosostudio.dbunifier.pipe;

import org.sosostudio.dbunifier.ColumnType;

public class NameType {

	private String name;

	private ColumnType type;

	public NameType(String name, ColumnType type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public ColumnType getType() {
		return type;
	}

}
