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
import java.sql.SQLException;

import org.sosostudio.dbking.ColumnType;

public class OracleFeature extends DbFeature {

	public OracleFeature(DatabaseMetaData dmd) {
		super(dmd);
	}

	@Override
	public String getStringDbType(int size) {
		size = Math.max(0, Math.min(size, ColumnType.MAX_STRING_SIZE));
		return "nvarchar2(" + size + ")";
	}

	@Override
	public String defaultSchema() {
		try {
			return dmd.getUserName();
		} catch (SQLException e) {
			return null;
		}
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

}
