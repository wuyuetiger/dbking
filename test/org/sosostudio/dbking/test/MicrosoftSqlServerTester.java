package org.sosostudio.dbking.test;

import org.junit.Test;
import org.sosostudio.dbking.DbUnifier;

public class MicrosoftSqlServerTester extends BaseTester {

	public MicrosoftSqlServerTester() {
		unifier = new DbUnifier("mssqlserver");
	}

	@Test
	public void testTypeBinary() {
		String typeName = "binary(200)";
		testSmallBlob(typeName);
	}

}