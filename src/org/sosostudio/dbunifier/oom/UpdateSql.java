/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2014 YU YUE, SOSO STUDIO, wuyuetiger@gmail.com
 *
 * License: GNU Lesser General Public License (LGPL)
 * 
 * Source code availability:
 *  https://github.com/wuyuetiger/db-unifier
 *  https://code.csdn.net/tigeryu/db-unifier
 *  https://git.oschina.net/db-unifier/db-unifier
 */

package org.sosostudio.dbunifier.oom;

public class UpdateSql {

	private String tableName;

	private UpdateKeyValueClause updateKeyValueClause;

	private ConditionClause conditionClause;

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

	public ConditionClause getConditionClause() {
		return conditionClause;
	}

	public UpdateSql setConditionClause(ConditionClause conditionClause) {
		this.conditionClause = conditionClause;
		return this;
	}

}
