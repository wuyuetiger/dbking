package org.sosostudio.dbunifier.test;

import org.sosostudio.dbunifier.DbUnifier;

public class Sybase15Tester extends BaseTester {

	public Sybase15Tester() {
		unifier = new DbUnifier("sybase");
	}

	@Override
	protected void createTable(String typeName) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + " null)");
	}

}
