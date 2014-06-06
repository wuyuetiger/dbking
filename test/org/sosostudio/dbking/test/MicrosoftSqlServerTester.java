package org.sosostudio.dbking.test;

import org.junit.Test;
import org.sosostudio.dbking.DbKing;

public class MicrosoftSqlServerTester extends BaseTester {

	public MicrosoftSqlServerTester() {
		dbKing = new DbKing("mssqlserver");
	}

	@Test
	public void testTypeBinary() {
		String typeName = "binary(200)";
		testSmallBlob(typeName);
	}

}