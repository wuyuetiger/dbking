package org.sosostudio.dbunifier.oom;

public class InsertSql {

	private String tableName;

	private InsertKeyValueClause insertKeyValueClause;

	public String getTableName() {
		return tableName;
	}

	public InsertSql setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public InsertKeyValueClause getInsertKeyValueClause() {
		return insertKeyValueClause;
	}

	public InsertSql setInsertKeyValueClause(
			InsertKeyValueClause insertKeyValueClause) {
		this.insertKeyValueClause = insertKeyValueClause;
		return this;
	}

}
