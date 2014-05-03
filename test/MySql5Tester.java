import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sosostudio.dbunifier.DbConfig;
import org.sosostudio.dbunifier.DbUnifier;

public class MySql5Tester extends BaseTester {

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	public void init() {
		DbConfig config = new DbConfig("com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306/test", "root", "55780029");
		unifier = new DbUnifier(config);
	}

	@Test
	public void testBigInt() {
		String typeName = "bigint";
		testInteger(typeName);
	}

	@Test
	public void testBinary() {
		String typeName = "binary(200)";
		testSmallBlob(typeName);
	}

	@Test
	public void testBit() {
		String typeName = "bit";
		testSmallInteger(typeName);
	}

	@Test
	public void testBlob() {
		String typeName = "blob";
		testBlob(typeName);
	}

	@Test
	public void testBool() {
		String typeName = "bool";
		testSmallInteger(typeName);
	}

	@Test
	public void testBoolean() {
		String typeName = "boolean";
		testSmallInteger(typeName);
	}

	@Test
	public void testChar() {
		String typeName = "char(10)";
		testString(typeName);
	}

	@Test
	public void testCharBinary() {
		String typeName = "char(50) binary";
		testString(typeName);
	}

	@Test
	public void testCharacter() {
		String typeName = "character(10)";
		testString(typeName);
	}

	@Test
	public void testCharacterVarying() {
		String typeName = "character varying(50)";
		testString(typeName);
	}

	@Test
	public void testDate() {
		String typeName = "date";
		testDate(typeName);
	}

	@Test
	public void testDatetime() {
		String typeName = "datetime";
		testTimestamp(typeName);
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
	public void testDouble() {
		String typeName = "double";
		testDecimal(typeName);
	}

	@Test
	public void testDoublePrecision() {
		String typeName = "double precision";
		testDecimal(typeName);
	}

	@Test
	public void testEnum() {
		String typeName = "enum('abcdefghij')";
		testString(typeName);
	}

	@Test
	public void testFixed() {
		String typeName = "fixed(10,2)";
		testDecimal(typeName);
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
	public void testLongBlob() {
		String typeName = "longblob";
		testBlob(typeName);
	}

	@Test
	public void testLongText() {
		String typeName = "longtext";
		testClob(typeName);
	}

	@Test
	public void testLongVarchar() {
		String typeName = "long varchar";
		testString(typeName);
	}

	@Test
	public void testMediumBlob() {
		String typeName = "mediumblob";
		testBlob(typeName);
	}

	@Test
	public void testMediumInt() {
		String typeName = "mediumint";
		testSmallInteger(typeName);
	}

	@Test
	public void testMediumText() {
		String typeName = "mediumtext";
		testClob(typeName);
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
	public void testReal() {
		String typeName = "real";
		testDecimal(typeName);
	}

	@Test
	public void testSet() {
		String typeName = "set('abcdefghij')";
		testString(typeName);
	}

	@Test
	public void testSmallInt() {
		String typeName = "smallint";
		testSmallInteger(typeName);
	}

	@Test
	public void testText() {
		String typeName = "text";
		testClob(typeName);
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
	public void testTinyBlob() {
		String typeName = "tinyblob";
		testSmallBlob(typeName);
	}

	@Test
	public void testTinyInt() {
		String typeName = "tinyint";
		testSmallInteger(typeName);
	}

	@Test
	public void testTinyText() {
		String typeName = "tinytext";
		testSmallClob(typeName);
	}

	@Test
	public void testVarchar() {
		String typeName = "varchar(50)";
		testString(typeName);
	}

	@Test
	public void testVarbinary() {
		String typeName = "varbinary(2000)";
		testSmallBlob(typeName);
	}

}
