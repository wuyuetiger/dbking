package org.sosostudio.dbunifier.test;

import org.sosostudio.dbunifier.DbUnifier;

public class PostgreSqlTester extends BaseTester {

	public PostgreSqlTester() {
		unifier = new DbUnifier("postgresql");
	}

}