package org.sosostudio.dbking.test;

import org.junit.Test;
import org.sosostudio.dbking.DbKing;

public class DmTester extends BaseTester {

	public DmTester() {
		dbKing = new DbKing("dm");
	}

	@Test
	public void testTypeNationalCharVarying() {
		String typeName = "national char varying(36)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNationalCharacterVarying() {
		String typeName = "national character varying(36)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNchar() {
		String typeName = "nchar(36)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNcharVarying() {
		String typeName = "nchar varying(36)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNvarchar() {
		String typeName = "nvarchar(36)";
		testChineseString12(typeName);
	}

}
