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

import org.sosostudio.dbking.exception.DbKingException;

public class MicrosoftSqlServerFeature extends DbFeature {

	public MicrosoftSqlServerFeature(DatabaseMetaData dmd) {
		super(dmd);
	}

	@Override
	public String getTimestampDbType() {
		return "datetime";
	}

	@Override
	public String getClobDbType() {
		return "text";
	}

	@Override
	public String getBlobDbType() {
		return "image";
	}

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		int majorVersion;
		try {
			majorVersion = dmd.getDatabaseMajorVersion();
		} catch (SQLException e) {
			throw new DbKingException(e);
		}
		if (majorVersion >= 9) {
			StringBuilder sb = new StringBuilder();
			if ("".equals(orderBySubSql)) {
				orderBySubSql = "order by (select 1)";
			}
			sb.append("select * from ( select tmp1_.*, row_number() over (")
					.append(orderBySubSql).append(") rownum_ from ( ")
					.append(mainSubSql)
					.append(" ) tmp1_ ) tmp2_ where rownum_ > ")
					.append(startPos).append(" and rownum_ <= ").append(endPos);
			return sb.toString();
		} else {
			return super.getPaginationSql(mainSubSql, orderBySubSql, startPos,
					endPos);
		}
	}

}
