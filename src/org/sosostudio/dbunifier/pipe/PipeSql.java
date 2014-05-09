package org.sosostudio.dbunifier.pipe;

import java.util.ArrayList;
import java.util.List;

public class PipeSql {

	private String insertSql;

	private List<NameType> insertNameTypeList = new ArrayList<NameType>();

	private boolean needUpdate = false;

	private String updateSql;

	private List<NameType> updateNameTypeList = new ArrayList<NameType>();

	public String getInsertSql() {
		return insertSql;
	}

	public PipeSql setInsertSql(String insertSql) {
		this.insertSql = insertSql;
		return this;
	}

	public List<NameType> getInsertNameTypeList() {
		return insertNameTypeList;
	}

	public PipeSql addInsertNameType(NameType insertNameType) {
		this.insertNameTypeList.add(insertNameType);
		return this;
	}

	public PipeSql addInsertNameTypeList(List<NameType> insertNameTypeList) {
		this.insertNameTypeList.addAll(insertNameTypeList);
		return this;
	}

	public boolean getNeedUpdate() {
		return needUpdate;
	}

	public PipeSql setNeedUpdate(boolean needUpdate) {
		this.needUpdate = needUpdate;
		return this;
	}

	public String getUpdateSql() {
		return updateSql;
	}

	public PipeSql setUpdateSql(String updateSql) {
		this.updateSql = updateSql;
		return this;
	}

	public List<NameType> getUpdateNameTypeList() {
		return updateNameTypeList;
	}

	public PipeSql addUpdateNameType(NameType updateNameType) {
		this.updateNameTypeList.add(updateNameType);
		return this;
	}

	public PipeSql addUpdateNameTypeList(List<NameType> updateNameTypeList) {
		this.updateNameTypeList.addAll(updateNameTypeList);
		return this;
	}

}
