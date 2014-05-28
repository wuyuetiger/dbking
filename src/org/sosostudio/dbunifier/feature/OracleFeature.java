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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.Encoding;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.DbUtil;

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
	public String getStringDbType(int size) {
		size = Math.max(0, Math.min(size, ColumnType.MAX_STRING_SIZE));
		return "varchar2(" + size + ")";
	}

	@Override
	public String getNStringDbType(int size) {
		size = Math.max(0, Math.min(size, ColumnType.MAX_STRING_SIZE));
		return "nvarchar2(" + size + ")";
	}

	@Override
	public Encoding getEncoding(Connection con) {
		String sql = "select userenv(‘language’) from dual";
		Statement statement = null;
		ResultSet rs = null;
		try {
			statement = con.createStatement();
			rs = statement.executeQuery(sql);
			if (rs.next()) {
				String language = rs.getString(1);
				if (language.endsWith("UTF8")) {
					return Encoding.UTF8;
				} else if (language.startsWith("ZHS")) {
					return Encoding.GBK;
				} else {
					throw new DbUnifierException("not support encoding: "
							+ language);
				}
			} else {
				throw new DbUnifierException("no encoding");
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeResultSet(rs);
			DbUtil.closeStatement(statement);
		}
	}

}
