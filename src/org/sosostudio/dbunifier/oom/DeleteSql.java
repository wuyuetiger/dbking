package org.sosostudio.dbunifier.oom;

public class DeleteSql {

	private String tableName;

	private ConditionClause conditionClause;

	public String getTableName() {
		return tableName;
	}

	public DeleteSql setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public ConditionClause getConditionClause() {
		return conditionClause;
	}

	public DeleteSql setConditionClause(ConditionClause conditionClause) {
		this.conditionClause = conditionClause;
		return this;
	}

}
