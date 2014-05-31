package org.sosostudio.dbunifier.test;

import org.junit.Test;
import org.sosostudio.dbunifier.DbUnifier;

public class MicrosoftSqlServer2012Tester extends BaseTester {

	public MicrosoftSqlServer2012Tester() {
		unifier = new DbUnifier("mssqlserver");
	}

	@Test
	public void testTypeBinary() {
		String typeName = "binary(200)";
		testSmallBlob(typeName);
	}

}