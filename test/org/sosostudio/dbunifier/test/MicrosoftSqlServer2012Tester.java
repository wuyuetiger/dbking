package org.sosostudio.dbunifier.test;

import org.sosostudio.dbunifier.DbUnifier;

public class MicrosoftSqlServer2012Tester extends BaseTester {

	public MicrosoftSqlServer2012Tester() {
		unifier = new DbUnifier("mssqlserver");
	}

}