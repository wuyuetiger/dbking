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
 *  https://git.oschina.net/db-unifier/db-unifier
 */

package org.sosostudio.dbunifier;

import java.io.Serializable;

import org.sosostudio.dbunifier.util.StringUtil;

public class Column implements Serializable {

	private static final long serialVersionUID = 2041593886142933287L;

	private String name;

	private ColumnType type;

	private boolean isNationalString = true;

	private int size = 50;

	private int precision = 10;

	private int scale = 2;

	private boolean isNullable = true;

	private boolean isPrimaryKey = false;

	public Column() {
	}

	public Column(String name, ColumnType type) {
		this.name = name;
		this.type = type;
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

	public boolean isNationalString() {
		return isNationalString;
	}

	public void setNationalString(boolean isNationalString) {
		this.isNationalString = isNationalString;
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

	public boolean isNullable() {
		return isNullable;
	}

	public Column setNullable(boolean isNullable) {
		this.isNullable = isNullable;
		return this;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public Column setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
		return this;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("\t[name = ").append(name)
				.append("]").append("[type = ").append(type).append("]")
				.append("[isNationalString = ").append(isNationalString)
				.append("]").append("[size = ").append(size).append("]")
				.append("[precision = ").append(precision).append("]")
				.append("[scale = ").append(scale).append("]")
				.append("[nullable = ").append(isNullable).append("]")
				.append("[isPrimaryKey = ").append(isPrimaryKey).append("]\n")
				.toString();
	}
}
