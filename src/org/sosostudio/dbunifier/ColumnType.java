/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2014 YU YUE, SOSO STUDIO, wuyuetiger@gmail.com
 *
 * License: GNU Lesser General Public License (LGPL)
 * 
 * Source code availability:
 *  https://github.com/wuyuetiger/db-unifier
 *  https://code.csdn.net/tigeryu/db-unifier
 */

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
