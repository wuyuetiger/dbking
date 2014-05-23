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
 */

package org.sosostudio.dbunifier.feature;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class OracleFeature extends DbFeature {

	@Override
	public String defaultCaps(String name) {
		return name.toUpperCase();
	}
	
	@Override
	public String getDatabaseSchema(DatabaseMetaData dmd) throws SQLException {
		return dmd.getUserName();
	}

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append("select * from ( select row_.*, rownum rownum_ from ( ")
				.append(mainSubSql).append(orderBySubSql)
				.append(" ) row_ where rownum <= ").append(endPos)
				.append(") where rownum_ > ").append(startPos);
		return sb.toString();
	}

	@Override
	public String getTimestampDbType() {
		return "timestamp";
	}

	@Override
	public String getClobDbType() {
		return "clob";
	}

	@Override
	public String getBlobDbType() {
		return "blob";
	}

}
