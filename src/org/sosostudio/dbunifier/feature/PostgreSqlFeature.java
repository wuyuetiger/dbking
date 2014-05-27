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

package org.sosostudio.dbunifier.feature;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class PostgreSqlFeature extends DbFeature {

	@Override
	public String defaultCaps(String name) {
		return name.toLowerCase();
	}

	@Override
	public String getDatabaseSchema(DatabaseMetaData dmd) throws SQLException {
		return "public";
	}

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append(mainSubSql).append(orderBySubSql).append(" limit ")
				.append(endPos - startPos).append(" offset ").append(startPos);
		return sb.toString();
	}

	@Override
	public String getStringDbType(int size, boolean isNationalString) {
		size = Math.max(0, Math.min(size, 2000));
		if (isNationalString) {
			return "national char varying(" + size + ")";
		} else {
			return "varchar(" + size + ")";
		}
	}

	@Override
	public String getClobDbType() {
		return "text";
	}

	@Override
	public String getBlobDbType() {
		return "bytea";
	}

}
