package org.sosostudio.dbunifier;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class Database {

	private DatabaseConfig databaseConfig;

	private Connection connection;

	public Database(DatabaseConfig databaseConfig) {
		this.databaseConfig = databaseConfig;
	}

	public Database(Connection connection) {
		this.connection = connection;
	}

	public String getDatabaseName() {
		Connection connection;
		if (this.connection == null) {
			connection = databaseConfig.getConnection();
		} else {
			connection = this.connection;
		}
		try {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			return databaseMetaData.getDatabaseProductName();
		} catch (SQLException e) {
			throw new DbunifierException(e);
		} finally {
			if (this.connection == null) {
				databaseConfig.closeConnection(connection);
			}
		}
	}

}
