/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2014 YU YUE, SOSO STUDIO, wuyuetiger@gmail.com
 *
 * License: GNU Lesser General Public License (LGPL)
 * 
 * Source code availability:
 *  https://github.com/wuyuetiger/dbking
 *  https://code.csdn.net/tigeryu/dbking
 *  https://git.oschina.net/db-unifier/dbking
 */

package org.sosostudio.dbking;

public enum ColumnType {

	STRING("String", "String"), NUMBER("Number", "BigDecimal"), TIMESTAMP(
			"Timestamp", "Timestamp"), CLOB("Clob", "char[]"), BLOB("Blob",
			"byte[]"), UNKNOWN("", "");

	public static final int MAX_STRING_SIZE = 2000;

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
