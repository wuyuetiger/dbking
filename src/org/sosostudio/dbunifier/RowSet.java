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

import java.util.ArrayList;
import java.util.List;

public class RowSet {

	private List<String> columnNameList = new ArrayList<String>();

	private List<Row> rowList = new ArrayList<Row>();

	private int pageSize = Integer.MAX_VALUE / 2;

	private int pageNumber = 1;

	private int totalRowCount = -1;

	private int totalPageCount = 1;

	public List<String> getColumnNameList() {
		return columnNameList;
	}

	public void addColumnName(String columnName) {
		columnNameList.add(columnName);
	}

	public void addRow(Row row) {
		rowList.add(row);
	}

	public int size() {
		return rowList.size();
	}

	public Row getRow(int i) {
		return rowList.get(i);
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	public int getTotalRowCount() {
		return totalRowCount;
	}

	public void setTotalRowCount(int totalRowCount) {
		this.totalRowCount = totalRowCount;
	}

	public int getTotalPageCount() {
		return totalPageCount;
	}

	public void setTotalPageCount(int totalPageCount) {
		this.totalPageCount = totalPageCount;
	}

	public Object[] toBeans(Class<?> clazz) {
		Object[] objs = new Object[rowList.size()];
		for (int i = 0; i < rowList.size(); i++) {
			Row row = rowList.get(i);
			objs[i] = row.toBean(clazz);
		}
		return objs;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n[pageSize = ").append(pageSize).append("][pageNumber = ")
				.append(pageNumber).append("][totalRowCount = ")
				.append(totalRowCount).append("][totalPageCount = ")
				.append(totalPageCount).append("][rowCount = ")
				.append(rowList.size()).append("]\n");
		for (Row row : rowList) {
			sb.append(row);
		}
		return sb.toString();
	}

}
