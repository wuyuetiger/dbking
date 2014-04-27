package org.sosostudio.dbunifier;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Row {

	private Map<String, Object> valueMap = new HashMap<String, Object>();

	private RowSet rowSet;

	public Row(RowSet rowSet) {
		this.rowSet = rowSet;
	}

	public Map<String, Object> getValueMap() {
		return this.valueMap;
	}

	public void addValue(String columnName, Object value) {
		this.valueMap.put(columnName, value);
	}

	public Object get(int i) {
		List<String> columnNameList = rowSet.getColumnNameList();
		String columnName = columnNameList.get(i - 1);
		return valueMap.get(columnName);
	}

	public Object get(String columnName) {
		columnName = columnName.toUpperCase();
		return valueMap.get(columnName);
	}

	public String getString(int i) {
		return (String) this.get(i);
	}

	public String getString(String columnName) {
		return (String) this.get(columnName);
	}

	public BigDecimal getNumber(int i) {
		return (BigDecimal) this.get(i);
	}

	public BigDecimal getNumber(String columnName) {
		return (BigDecimal) this.get(columnName);
	}

	public Timestamp getTimestamp(int i) {
		return (Timestamp) this.get(i);
	}

	public Timestamp getTimestamp(String columnName) {
		return (Timestamp) this.get(columnName);
	}

	public String getClob(int i) {
		return (String) this.get(i);
	}

	public String getClob(String columnName) {
		return (String) this.get(columnName);
	}

	public byte[] getBlob(int i) {
		return (byte[]) this.get(i);
	}

	public byte[] getBlob(String columnName) {
		return (byte[]) this.get(columnName);
	}

}
