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
		return this.columnNameList;
	}

	public void addColumnName(String columnName) {
		this.columnNameList.add(columnName);
	}

	public List<Row> getRowList() {
		return this.rowList;
	}

	public void addRow(Row row) {
		this.rowList.add(row);
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

}
