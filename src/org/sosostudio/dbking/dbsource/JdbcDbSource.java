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

package org.sosostudio.dbking.dbsource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.sosostudio.dbking.exception.DbKingException;

public class JdbcDbSource implements DbSource {

	private String databaseDriver;

	private String databaseUrl;

	private String username;

	private String password;

	private String schema;

	public JdbcDbSource(String databaseDriver, String databaseUrl,
			String username, String password, String schema) {
		this.databaseDriver = databaseDriver;
		this.databaseUrl = databaseUrl;
		this.username = username;
		this.password = password;
		this.schema = schema;
	}

	public JdbcDbSource(String databaseDriver, String databaseUrl,
			String username, String password) {
		this.databaseDriver = databaseDriver;
		this.databaseUrl = databaseUrl;
		this.username = username;
		this.password = password;
	}

	public JdbcDbSource(String databaseDriver, String databaseUrl, String schema) {
		this.databaseDriver = databaseDriver;
		this.databaseUrl = databaseUrl;
		this.schema = schema;
	}

	public JdbcDbSource(String databaseDriver, String databaseUrl) {
		this.databaseDriver = databaseDriver;
		this.databaseUrl = databaseUrl;
	}

	@Override
	public Connection getConnection() {
		if (username == null) {
			try {
				Class.forName(databaseDriver);
				return DriverManager.getConnection(databaseUrl);
			} catch (ClassNotFoundException e) {
				throw new DbKingException(e);
			} catch (SQLException e) {
				throw new DbKingException(e);
			}
		} else {
			try {
				Class.forName(databaseDriver);
				return DriverManager.getConnection(databaseUrl, username,
						password);
			} catch (ClassNotFoundException e) {
				throw new DbKingException(e);
			} catch (SQLException e) {
				throw new DbKingException(e);
			}
		}
	}

	@Override
	public String getSchema() {
		return schema;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("\ndatabaseUrl: ")
				.append(databaseUrl).append("\nusername: ").append(username)
				.append("\n").toString();
	}

}
