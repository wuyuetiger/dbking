package org.sosostudio.dbunifier.test;

import org.sosostudio.dbunifier.DbUnifier;

public class PostgreSql9Tester extends BaseTester {

	public PostgreSql9Tester() {
		unifier = new DbUnifier("postgresql");
	}

}