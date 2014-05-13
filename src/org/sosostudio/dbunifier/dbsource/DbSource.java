package org.sosostudio.dbunifier.dbsource;

import java.sql.Connection;

public interface DbSource {

	public Connection getConnection();

}
