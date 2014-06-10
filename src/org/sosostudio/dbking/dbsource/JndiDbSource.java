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
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.sosostudio.dbking.exception.DbKingException;

public class JndiDbSource implements DbSource {

	private String jndi;

	private String username;

	private String password;

	private String schema;

	public JndiDbSource(String jndi, String schema) {
		this.jndi = jndi;
		this.schema = schema;
	}

	public JndiDbSource(String jndi) {
		this.jndi = jndi;
	}

	public JndiDbSource(String jndi, String username, String password,
			String schema) {
		this.jndi = jndi;
		this.username = username;
		this.password = password;
		this.schema = schema;
	}

	public JndiDbSource(String jndi, String username, String password) {
		this.jndi = jndi;
		this.username = username;
		this.password = password;
	}

	@Override
	public Connection getConnection() {
		if (username == null) {
			try {
				InitialContext initialContext = new InitialContext();
				DataSource dataSource = (DataSource) initialContext
						.lookup(jndi);
				return dataSource.getConnection();
			} catch (NamingException e) {
				throw new DbKingException(e);
			} catch (SQLException e) {
				throw new DbKingException(e);
			}
		} else {
			try {
				InitialContext initialContext = new InitialContext();
				DataSource dataSource = (DataSource) initialContext
						.lookup(jndi);
				return dataSource.getConnection(username, password);
			} catch (NamingException e) {
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
		return new StringBuilder().append("\njndi: ").append(jndi).append("\n")
				.toString();
	}

}
