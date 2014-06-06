package org.sosostudio.dbking.test;

import org.sosostudio.dbking.DbUnifier;

public class PostgreSqlTester extends BaseTester {

	public PostgreSqlTester() {
		unifier = new DbUnifier("postgresql");
	}

}