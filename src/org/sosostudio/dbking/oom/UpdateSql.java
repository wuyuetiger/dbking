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

public class UpdateSql {

	private String tableName;

	private UpdateKeyValueClause updateKeyValueClause;

	private WhereClause whereClause;

	public String getTableName() {
		return tableName;
	}

	public UpdateSql setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public UpdateKeyValueClause getUpdateKeyValueClause() {
		return updateKeyValueClause;
	}

	public UpdateSql setUpdateKeyValueClause(
			UpdateKeyValueClause updateKeyValueClause) {
		this.updateKeyValueClause = updateKeyValueClause;
		return this;
	}

	public WhereClause getWhereClause() {
		return whereClause;
	}

	public UpdateSql setWhereClause(WhereClause whereClause) {
		this.whereClause = whereClause;
		return this;
	}

}
