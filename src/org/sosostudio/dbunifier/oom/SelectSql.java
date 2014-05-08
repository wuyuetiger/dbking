package org.sosostudio.dbunifier.oom;

public class SelectSql {

	private String tableName;

	private String columns;

	private ConditionClause conditionClause;

	private ExtraClause extraClause;

	private OrderByClause orderByClause;

	public String getTableName() {
		return tableName;
	}

	public SelectSql setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public String getColumns() {
		return columns;
	}

	public SelectSql setColumns(String columns) {
		this.columns = columns;
		return this;
	}

	public ConditionClause getConditionClause() {
		return conditionClause;
	}

	public SelectSql setConditionClause(ConditionClause conditionClause) {
		this.conditionClause = conditionClause;
		return this;
	}

	public ExtraClause getExtraClause() {
		return extraClause;
	}

	public SelectSql setExtraClause(ExtraClause extraClause) {
		this.extraClause = extraClause;
		return this;
	}

	public OrderByClause getOrderByClause() {
		return orderByClause;
	}

	public SelectSql setOrderByClause(OrderByClause orderByClause) {
		this.orderByClause = orderByClause;
		return this;
	}

}
