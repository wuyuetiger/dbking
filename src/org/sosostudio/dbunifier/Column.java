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

import java.io.Serializable;

import org.sosostudio.dbunifier.util.StringUtil;

public class Column implements Serializable {

	private static final long serialVersionUID = 2041593886142933287L;

	private String name;

	private ColumnType type;

	private int size = 50;

	private int precision = 10;

	private int scale = 2;

	private boolean nullable;

	private boolean isPrimaryKey;

	public Column() {
	}

	public Column(String name, ColumnType type, boolean nullable,
			boolean isPrimaryKey) {
		this.name = name;
		this.type = type;
		this.nullable = nullable;
		this.isPrimaryKey = isPrimaryKey;
	}

	public String getName() {
		return name;
	}

	public String getDefinationName() {
		return StringUtil.getDefinationName(name);
	}

	public String getVariableName() {
		return StringUtil.getVariableName(name);
	}

	public Column setName(String name) {
		this.name = name;
		return this;
	}

	public ColumnType getType() {
		return type;
	}

	public Column setType(ColumnType type) {
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

	public int getPrecision() {
		return precision;
	}

	public Column setPrecision(int precision) {
		this.precision = precision;
		return this;
	}

	public int getScale() {
		return scale;
	}

	public Column setScale(int scale) {
		this.scale = scale;
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
		return new StringBuilder().append("\t[name = ").append(name)
				.append("]").append("[type = ").append(type).append("]")
				.append("[size = ").append(size).append("]")
				.append("[precision = ").append(precision).append("]")
				.append("[scale = ").append(scale).append("]")
				.append("[nullable = ").append(nullable).append("]")
				.append("[isPrimaryKey = ").append(isPrimaryKey).append("]\n")
				.toString();
	}
}
