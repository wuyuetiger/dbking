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
