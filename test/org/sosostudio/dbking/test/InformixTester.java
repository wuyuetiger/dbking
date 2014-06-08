package org.sosostudio.dbking.test;

import org.junit.Test;
import org.sosostudio.dbking.DbKing;

public class InformixTester extends BaseTester {

	public InformixTester() {
		dbKing = new DbKing("informix");
	}

	@Test
	public void testTypeDatetime() {
		String typeName = "datetime year to fraction";
		testTimestamp(typeName);
	}

	@Test
	public void testTypeMoney() {
		String typeName = "money";
		testDecimal(typeName);
	}

	@Test
	public void testTypeNchar() {
		String typeName = "nchar(36)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNvarchar() {
		String typeName = "nvarchar(36)";
		testChineseString12(typeName);
	}

}
