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

package org.sosostudio.dbunifier.dbsource;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.sosostudio.dbunifier.util.DbUnifierException;

public class JndiDbSource extends DbSource {

	private String jndi;

	private String username;

	private String password;

	public JndiDbSource(String jndi) {
		this.jndi = jndi;
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
				throw new DbUnifierException(e);
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			}
		} else {
			try {
				InitialContext initialContext = new InitialContext();
				DataSource dataSource = (DataSource) initialContext
						.lookup(this.jndi);
				return dataSource.getConnection(this.username, this.password);
			} catch (NamingException e) {
				throw new DbUnifierException(e);
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	@Override
	public Connection getConnection(String username, String password) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return new StringBuilder().append("\njndi: ").append(jndi).append("\n")
				.toString();
	}

}
