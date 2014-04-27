import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sosostudio.dbunifier.DbConfig;
import org.sosostudio.dbunifier.DbUnifier;

public class OracleTester extends BaseTester {

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	public void init() {
		DbConfig config = new DbConfig("oracle.jdbc.driver.OracleDriver",
				"jdbc:oracle:thin:@localhost:1521:orcl", "system", "55780029");
		unifier = new DbUnifier(config);
	}

	@Test
	public void testBlob() {
		String typeName = "blob";
		testStandardBlob(typeName);
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
	public void testClob() {
		String typeName = "clob";
		testClob(typeName);
	}

	@Test
	public void testDate() {
		String typeName = "date";
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
	public void testLong() {
		String typeName = "long";
		testClob(typeName);
	}

	@Test
	public void testLongRaw() {
		String typeName = "long raw";
		testStandardBlob(typeName);
	}

	@Test
	public void testLongVarchar() {
		String typeName = "long varchar";
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
	public void testNclob() {
		String typeName = "nclob";
		testClob(typeName);
	}

	@Test
	public void testNumber() {
		String typeName = "number(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testNumeric() {
		String typeName = "numeric(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testNvarchar2() {
		String typeName = "nvarchar2(50)";
		testString(typeName);
	}

	@Test
	public void testRaw() {
		String typeName = "raw(2000)";
		testSmallBlob(typeName);
	}

	@Test
	public void testReal() {
		String typeName = "real";
		testDecimal(typeName);
	}

	@Test
	public void testSmallInt() {
		String typeName = "smallint";
		testInteger(typeName);
	}

	@Test
	public void testTimestamp() {
		String typeName = "timestamp";
		testTimestamp(typeName);
	}

	@Test
	public void testVarchar() {
		String typeName = "varchar(50)";
		testString(typeName);
	}

	@Test
	public void testVarchar2() {
		String typeName = "varchar2(50)";
		testString(typeName);
	}

}
