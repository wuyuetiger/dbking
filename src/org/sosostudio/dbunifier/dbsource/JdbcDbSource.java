package org.sosostudio.dbunifier.dbsource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.sosostudio.dbunifier.util.DbUnifierException;

public class JdbcDbSource implements DbSource {

	private String databaseDriver;

	private String databaseUrl;

	private String username;

	private String password;

	public JdbcDbSource(String databaseDriver, String databaseUrl,
			String username, String password) {
		this.databaseDriver = databaseDriver;
		this.databaseUrl = databaseUrl;
		this.username = username;
		this.password = password;
	}

	public JdbcDbSource(String databaseDriver, String databaseUrl) {
		this.databaseDriver = databaseDriver;
		this.databaseUrl = databaseUrl;
	}

	public Connection getConnection() {
		if (username == null) {
			try {
				Class.forName(databaseDriver);
				return DriverManager.getConnection(databaseUrl, username,
						password);
			} catch (ClassNotFoundException e) {
				throw new DbUnifierException(e);
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			}
		} else {
			try {
				Class.forName(databaseDriver);
				return DriverManager.getConnection(databaseUrl);
			} catch (ClassNotFoundException e) {
				throw new DbUnifierException(e);
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			}
		}
	}

}
