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
import org.sosostudio.dbking.exception.DbKingException;

public class Db2Feature extends DbFeature {

	public Db2Feature(DatabaseMetaData dmd) {
		super(dmd);
	}

	@Override
	public String getStringDbType(int size) {
		int majorVersion;
		try {
			majorVersion = dmd.getDatabaseMajorVersion();
		} catch (SQLException e) {
			throw new DbKingException(e);
		}
		if (majorVersion >= 10) {
			return super.getStringDbType(size);
		} else {
			size = Math.max(0, Math.min(size, ColumnType.MAX_STRING_SIZE));
			return "varchar(" + size + ")";
		}
	}

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		String version;
		try {
			version = dmd.getDatabaseProductVersion();
		} catch (SQLException e) {
			throw new DbKingException(e);
		}
		int ver = Integer.parseInt(version.substring("SQL".length()));
		if (ver >= 9072) {
			StringBuilder sb = new StringBuilder();
			sb.append(mainSubSql).append(orderBySubSql).append(" limit ")
					.append(endPos - startPos).append(" offset ")
					.append(startPos);
			return sb.toString();
		} else {
			StringBuilder sb = new StringBuilder();
			sb.append(
					"select * from ( select tmp1_.*, row_number() over() as rownum_ from ( ")
					.append(mainSubSql).append(orderBySubSql)
					.append(" ) tmp1_ ) tmp2_ where rownum_ > ")
					.append(startPos).append(" and rownum_ <= ").append(endPos);
			return sb.toString();
		}
	}

}
