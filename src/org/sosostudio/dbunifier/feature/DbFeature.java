package org.sosostudio.dbunifier.feature;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.sosostudio.dbunifier.DbUnifierException;

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

	public String getDatabaseSchema(DatabaseMetaData databaseMetaData)
			throws SQLException {
		return null;
	}

	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		return null;
	}

}
