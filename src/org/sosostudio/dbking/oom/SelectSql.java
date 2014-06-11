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

package org.sosostudio.dbking.oom;

public class SelectSql {

	private String tableName;

	private String columns = "*";

	private WhereClause whereClause;

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

	public WhereClause getWhereClause() {
		return whereClause;
	}

	public SelectSql setWhereClause(WhereClause whereClause) {
		this.whereClause = whereClause;
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
