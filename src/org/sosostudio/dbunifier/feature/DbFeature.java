package org.sosostudio.dbunifier.feature;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.sosostudio.dbunifier.Values;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.DbUtil;

public abstract class DbFeature {

	public final static String DB_ORACLE = "Oracle";

	public final static String DB_MSSQLSERVER = "Microsoft SQL Server";

	public final static String DB_MYSQL = "MySQL";

	public final static String DB_POSTGRESQL = "PostgreSQL";

	public static DbFeature getInstance(DatabaseMetaData databaseMetaData)
			throws SQLException {
		String name = databaseMetaData.getDatabaseProductName();
		int databaseVersion = databaseMetaData.getDatabaseMajorVersion();
		if (DB_ORACLE.equals(name)) {
			return new OracleFeature();
		} else if (DB_MSSQLSERVER.equals(name)) {
			if (databaseVersion >= 9) {
				return new MicrosoftSqlServer2005Feature();
			} else {
				return new MicrosoftSqlServer2000Feature();
			}
		} else if (DB_MYSQL.equals(name)) {
			return new MySqlFeature();
		} else if (DB_POSTGRESQL.equals(name)) {
			return new PostgreSqlFeature();
		} else {
			throw new DbUnifierException("not support database");
		}
	}

	public String getDatabaseSchema(DatabaseMetaData dmd) throws SQLException {
		return null;
	}

	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		return null;
	}

	public String getStringDbType(int size) {
		size = Math.max(0, Math.min(size, 2000));
		return "varchar(" + size + ")";
	}

	public String getNumberDbType(int precision, int scale) {
		return "numeric(" + precision + "," + scale + ")";
	}

	public abstract String getTimestampDbType();

	public abstract String getClobDbType();

	public abstract String getBlobDbType();

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
				throw new DbUnifierException("no resultset");
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeResultSet(rs);
			DbUtil.closeStatement(ps);
		}
	}

}
