import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sosostudio.dbunifier.DbConfig;
import org.sosostudio.dbunifier.DbUnifier;

public class MicrosoftSqlServer2012Tester extends BaseTester {

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	public void init() {
		DbConfig config = new DbConfig(
				"com.microsoft.sqlserver.jdbc.SQLServerDriver",
				"jdbc:sqlserver://localhost:1433;DatabaseName=master", "sa",
				"55780029");
		unifier = new DbUnifier(config);
	}

	@Test
	public void testBigInt() {
		String typeName = "bigint";
		testInteger(typeName);
	}

	@Test
	public void testBinaryVarying() {
		String typeName = "binary varying(4000)";
		testBlob(typeName);
	}

	@Test
	public void testBinary() {
		String typeName = "binary(2000)";
		testSmallBlob(typeName);
	}

	@Test
	public void testBit() {
		String typeName = "bit";
		testSmallInteger(typeName);
	}

	@Test
	public void testChar() {
		String typeName = "char(10)";
		testString(typeName);
	}

	@Test
	public void testCharVarying() {
		String typeName = "char varying(50)";
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
	public void testDatetime2() {
		String typeName = "datetime2";
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
	public void testDoublePrecision() {
		String typeName = "double precision";
		testDecimal(typeName);
	}

	@Test
	public void testFloat() {
		String typeName = "float";
		testSmallDecimal(typeName);
	}

	@Test
	public void testImage() {
		String typeName = "image";
		testBlob(typeName);
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
	public void testMoney() {
		String typeName = "money";
		testMoney(typeName);
	}

	@Test
	public void testNationalCharVarying() {
		String typeName = "national char varying(50)";
		testString(typeName);
	}

	@Test
	public void testNationalCharacterVarying() {
		String typeName = "national character varying(50)";
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
	public void testNvarchar() {
		String typeName = "nvarchar(50)";
		testString(typeName);
	}

	@Test
	public void testSmallDatetime() {
		String typeName = "smalldatetime";
		testDate(typeName);
	}

	@Test
	public void testSmallInt() {
		String typeName = "smallint";
		testSmallInteger(typeName);
	}

	@Test
	public void testSmallMoney() {
		String typeName = "smallmoney";
		testMoney(typeName);
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
	public void testTinyInt() {
		String typeName = "tinyint";
		testSmallInteger(typeName);
	}

	@Test
	public void testVarbinary() {
		String typeName = "varbinary(2000)";
		testSmallBlob(typeName);
	}

	@Test
	public void testVarchar() {
		String typeName = "varchar(50)";
		testString(typeName);
	}

}
