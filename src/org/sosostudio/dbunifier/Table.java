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
import java.util.ArrayList;
import java.util.List;

import org.sosostudio.dbunifier.util.StringUtil;

public class Table implements Comparable<Table>, Serializable {

	private static final long serialVersionUID = 7642707849725992210L;

	// it's used to sort tables with foreign key relations
	private int seq = 0;

	private String name;

	private List<Column> columnList = new ArrayList<Column>();

	public Table() {
	}

	public Table(String name) {
		this.name = name;
	}

	public int getSeq() {
		return seq;
	}

	public Table setSeq(int seq) {
		this.seq = seq;
		return this;
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

	public Table setName(String name) {
		this.name = name;
		return this;
	}

	public List<Column> getColumnList() {
		return this.columnList;
	}

	public Table addColumn(Column column) {
		this.columnList.add(column);
		return this;
	}

	public Table addColumnList(List<Column> columnList) {
		this.columnList.addAll(columnList);
		return this;
	}

	@Override
	public int compareTo(Table table) {
		return seq - table.getSeq();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n[name = ").append(name).append("]\n");
		for (Column column : columnList) {
			sb.append(column);
		}
		return sb.toString();
	}

}
