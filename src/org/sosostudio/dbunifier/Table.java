package org.sosostudio.dbunifier;

import java.util.ArrayList;
import java.util.List;

public class Table {

	private String name;

	private List<Column> columnList = new ArrayList<Column>();

	public Table() {
	}

	public Table(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
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
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n[name = " + name + "]\n");
		for (Column column : columnList) {
			sb.append(column);
		}
		return sb.toString();
	}

}
