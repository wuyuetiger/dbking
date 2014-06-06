package org.sosostudio.dbking.test;

import org.sosostudio.dbking.DbUnifier;

public class SybaseTester extends BaseTester {

	public SybaseTester() {
		unifier = new DbUnifier("sybase");
	}

	@Override
	protected void createTable(String typeName) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + " null)");
	}

}
