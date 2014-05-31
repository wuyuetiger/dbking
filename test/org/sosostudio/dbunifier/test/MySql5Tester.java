package org.sosostudio.dbunifier.test;

import org.junit.Test;
import org.sosostudio.dbunifier.DbUnifier;

public class MySql5Tester extends BaseTester {

	public MySql5Tester() {
		unifier = new DbUnifier("mysql");
	}

	@Test
	public void testTypeBinary() {
		String typeName = "binary(200)";
		super.testSmallBlob(typeName);
	}

	@Test
	public void testTypeTimestamp() {
		String typeName = "timestamp null";
		testTimestamp(typeName);
	}

}
