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

package org.sosostudio.dbking.feature;

import java.sql.DatabaseMetaData;

import org.sosostudio.dbking.ColumnType;

public class PostgreSqlFeature extends DbFeature {

	public PostgreSqlFeature(DatabaseMetaData dmd) {
		super(dmd);
	}

	@Override
	public String getStringDbType(int size) {
		size = Math.max(0, Math.min(size, ColumnType.MAX_STRING_SIZE));
		return "varchar(" + size + ")";
	}

	@Override
	public String getClobDbType() {
		return "text";
	}

	@Override
	public String getBlobDbType() {
		return "bytea";
	}

	@Override
	public String defaultSchema() {
		return "public";
	}

	@Override
	public String defaultCaps(String name) {
		return name.toLowerCase();
	}

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append(mainSubSql).append(orderBySubSql).append(" limit ")
				.append(endPos - startPos).append(" offset ").append(startPos);
		return sb.toString();
	}

}
