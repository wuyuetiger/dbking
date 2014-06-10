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

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.sosostudio.dbking.ColumnType;
import org.sosostudio.dbking.Values;
import org.sosostudio.dbking.exception.DbKingException;
import org.sosostudio.dbking.util.DbUtil;

public class DbFeature {

	public final static String ORACLE = "Oracle";

	public final static String MSSQLSERVER = "Microsoft SQL Server";

	public final static String MYSQL = "MySQL";

	public final static String DB2 = "DB2";

	public final static String SYBASE = "Adaptive Server Enterprise";

	public final static String INFORMIX = "Informix Dynamic Server";

	public final static String POSTGRESQL = "PostgreSQL";

	public final static String DERBY = "Apache Derby";

	public final static String KINGBASE = "KingbaseES";

	public final static String DM = "DM DBMS";

	protected DatabaseMetaData dmd;

	protected DbFeature(DatabaseMetaData dmd) {
		this.dmd = dmd;
	}

	public static DbFeature getInstance(DatabaseMetaData dmd)
			throws SQLException {
		String name = dmd.getDatabaseProductName();
		if (ORACLE.equals(name)) {
			return new OracleFeature(dmd);
		} else if (MSSQLSERVER.equals(name)) {
			return new MicrosoftSqlServerFeature(dmd);
		} else if (MYSQL.equals(name)) {
			return new MySqlFeature(dmd);
		} else if (name.startsWith(DB2)) {
			return new Db2Feature(dmd);
		} else if (SYBASE.equals(name)) {
			return new SybaseFeature(dmd);
		} else if (INFORMIX.equals(name)) {
			return new InformixFeature(dmd);
		} else if (POSTGRESQL.equals(name)) {
			return new PostgreSqlFeature(dmd);
		} else if (DERBY.equals(name)) {
			return new DerbyFeature(dmd);
		} else if (KINGBASE.equals(name)) {
			return new KingbaseFeature(dmd);
		} else if (DM.equals(name)) {
			return new DmFeature(dmd);
		} else {
			System.out
					.println("dbking will use default db feature without test");
			return new DbFeature(dmd);
		}
	}

	public String getStringDbType(int size) {
		size = Math.max(0, Math.min(size, ColumnType.MAX_STRING_SIZE));
		return "nvarchar(" + size + ")";
	}

	public String getNumberDbType(int precision, int scale) {
		return "numeric(" + Math.max(0, precision) + "," + Math.max(0, scale)
				+ ")";
	}

	public String getTimestampDbType() {
		return "timestamp";
	}

	public String getClobDbType() {
		return "clob";
	}

	public String getBlobDbType() {
		return "blob";
	}

	public String defaultSchema() {
		return null;
	}

	public String defaultCaps(String name) {
		return name.toUpperCase();
	}

	public boolean allowNullByDefault() {
		return true;
	}

	public String constraintClause(StringBuilder sbPk) {
		StringBuilder sb = new StringBuilder();
		String uuid16 = UUID.randomUUID().toString().replace("-", "");
		BigInteger big = new BigInteger(uuid16, 16);
		String uuid36 = big.toString(36);
		sb.append(", constraint ").append("PK_").append(uuid36)
				.append(" primary key (").append(sbPk).append(")");
		return sb.toString();
	}

	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		return null;
	}

	public int getTotalRowCount(Connection con, String mainSubSql, Values values) {
		String countSql = "select count(*) from (" + mainSubSql + ") t";
		DbUtil.printSql(countSql, values);
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(countSql);
			DbUtil.setColumnValue(ps, 1, values);
			rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getBigDecimal(1).intValue();
			} else {
				throw new DbKingException("no resultset");
			}
		} catch (SQLException e) {
			throw new DbKingException(e);
		} finally {
			DbUtil.closeResultSet(rs);
			DbUtil.closeStatement(ps);
		}
	}

}
