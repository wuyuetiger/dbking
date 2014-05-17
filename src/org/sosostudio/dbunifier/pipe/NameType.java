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
