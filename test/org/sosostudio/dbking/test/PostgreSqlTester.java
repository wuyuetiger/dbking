package org.sosostudio.dbking.test;

import org.sosostudio.dbking.DbKing;

public class PostgreSqlTester extends BaseTester {

	public PostgreSqlTester() {
		dbKing = new DbKing("postgresql");
	}

}