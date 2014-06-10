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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class RowList extends ArrayList<Row> {

	private List<String> columnNameList = new ArrayList<String>();

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

	public String getSingleString() {
		if (size() > 0) {
			return get(0).getString(1);
		} else {
			return null;
		}
	}

	public BigDecimal getSingleNumber() {
		if (size() > 0) {
			return get(0).getNumber(1);
		} else {
			return null;
		}
	}

	public Timestamp getSingleTimestamp() {
		if (size() > 0) {
			return get(0).getTimestamp(1);
		} else {
			return null;
		}
	}

	public String getSingleClob() {
		if (size() > 0) {
			return get(0).getClob(1);
		} else {
			return null;
		}
	}

	public byte[] getSingleBlob() {
		if (size() > 0) {
			return get(0).getBlob(1);
		} else {
			return null;
		}
	}

	public Object[] toBeans(Class<?> clazz) {
		Object[] objs = new Object[size()];
		for (int i = 0; i < size(); i++) {
			Row row = get(i);
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
				.append(totalPageCount).append("][rowCount = ").append(size())
				.append("]\n");
		for (Row row : this) {
			sb.append(row);
		}
		return sb.toString();
	}

}
