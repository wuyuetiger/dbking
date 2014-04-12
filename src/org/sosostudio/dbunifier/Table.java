package org.sosostudio.dbunifier;

import java.util.ArrayList;
import java.util.List;

public class Table {

	private String name;

	private List<Column> columnList = new ArrayList<Column>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Column> getColumnList() {
		return this.columnList;
	}

	public void addColumn(Column column) {
		this.columnList.add(column);
	}

	public void addColumnList(List<Column> columnList) {
		this.columnList.addAll(columnList);
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
