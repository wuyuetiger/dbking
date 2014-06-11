package org.sosostudio.dbking.test;

import org.sosostudio.dbking.DbKing;

public class SybaseTester extends BaseTester {

	public SybaseTester() {
		dbKing = new DbKing("sybase");
	}

	@Override
	protected void createTable(String typeName) {
		dbKing.execute("create table " + tableName + " (" + columnName
				+ " " + typeName + " null)");
	}

}
