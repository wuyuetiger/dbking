package org.sosostudio.dbunifier.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.dbsource.DbSource;
import org.sosostudio.dbunifier.dbsource.JdbcDbSource;

public class PostgreSql9Tester extends BaseTester {

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	public void init() {
		DbSource dbSource = new JdbcDbSource("org.postgresql.Driver",
				"jdbc:postgresql://localhost:5432/postgres", "postgres",
				"55780029");
		unifier = new DbUnifier(dbSource);
	}

	@Test
	public void testBytea() {
		String typeName = "bytea";
		testBlob(typeName);
	}

	@Test
	public void testChar() {
		String typeName = "char(10)";
		testString(typeName);
	}

	@Test
	public void testCharacter() {
		String typeName = "character(10)";
		testString(typeName);
	}

	@Test
	public void testDate() {
		String typeName = "date";
		testDate(typeName);
	}

	@Test
	public void testDec() {
		String typeName = "dec(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testDecimal() {
		String typeName = "decimal(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testFloat() {
		String typeName = "float";
		testSmallDecimal(typeName);
	}

	@Test
	public void testInt() {
		String typeName = "int";
		testInteger(typeName);
	}

	@Test
	public void testInteger() {
		String typeName = "integer";
		testInteger(typeName);
	}

	@Test
	public void testNationalChar() {
		String typeName = "national char(10)";
		testString(typeName);
	}

	@Test
	public void testNationalCharVarying() {
		String typeName = "national char varying(50)";
		testString(typeName);
	}

	@Test
	public void testNationalCharacter() {
		String typeName = "national character(10)";
		testString(typeName);
	}

	@Test
	public void testNationalCharacterVarying() {
		String typeName = "national character varying(50)";
		testString(typeName);
	}

	@Test
	public void testNchar() {
		String typeName = "nchar(10)";
		testString(typeName);
	}

	@Test
	public void testNcharVarying() {
		String typeName = "nchar varying(50)";
		testString(typeName);
	}

	@Test
	public void testNumeric() {
		String typeName = "numeric(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testTime() {
		String typeName = "time";
		testTime(typeName);
	}

	@Test
	public void testTimestamp() {
		String typeName = "timestamp null";
		testTimestamp(typeName);
	}

	@Test
	public void testVarchar() {
		String typeName = "varchar(50)";
		testString(typeName);
	}

}
