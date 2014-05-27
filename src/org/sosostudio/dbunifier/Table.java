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
import java.util.ArrayList;
import java.util.List;

import org.sosostudio.dbunifier.util.StringUtil;

public class Table implements Comparable<Table>, Serializable {

	private static final long serialVersionUID = 7642707849725992210L;

	// it's used to sort tables with foreign key relations
	private int depth = 0;

	private List<Table> parentTableList = new ArrayList<Table>();

	private boolean isTable = true;

	private String name;

	private List<Column> columnList = new ArrayList<Column>();

	public Table(String name) {
		this.name = name;
	}

	protected int getDepth() {
		return depth;
	}

	protected Table setDepth(int depth) {
		this.depth = depth;
		return this;
	}

	protected List<Table> getParentTableList() {
		return parentTableList;
	}

	public Table addParentTable(Table table) {
		parentTableList.add(table);
		return this;
	}

	public boolean isTable() {
		return isTable;
	}

	public Table setTable(boolean isTable) {
		this.isTable = isTable;
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
		return columnList;
	}

	public Table addColumn(Column column) {
		columnList.add(column);
		return this;
	}

	public Table addColumnList(List<Column> columnList) {
		this.columnList.addAll(columnList);
		return this;
	}

	@Override
	public int compareTo(Table table) {
		return depth - table.getDepth();
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
