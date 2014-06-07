package org.sosostudio.dbking.test;

import org.sosostudio.dbking.DbKing;

public class MariaDbTester extends MySqlTester {

	public MariaDbTester() {
		dbKing = new DbKing("mariadb");
	}

}
