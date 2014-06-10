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

import javax.sql.DataSource;

import org.sosostudio.dbking.exception.DbKingException;

public class WrappedDbSource implements DbSource {

	private DataSource dataSource;

	private String schema;

	public WrappedDbSource(DataSource dataSource, String schema) {
		this.dataSource = dataSource;
		this.schema = schema;
	}

	public WrappedDbSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new DbKingException(e);
		}
	}

	@Override
	public String getSchema() {
		return schema;
	}

}
