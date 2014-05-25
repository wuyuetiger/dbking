package org.sosostudio.dbunifier.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

import junit.framework.TestCase;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sosostudio.dbunifier.Column;
import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Row;
import org.sosostudio.dbunifier.RowSet;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.oom.ConditionClause;
import org.sosostudio.dbunifier.oom.Direction;
import org.sosostudio.dbunifier.oom.InsertKeyValueClause;
import org.sosostudio.dbunifier.oom.InsertSql;
import org.sosostudio.dbunifier.oom.LogicalOp;
import org.sosostudio.dbunifier.oom.OrderByClause;
import org.sosostudio.dbunifier.oom.RelationOp;
import org.sosostudio.dbunifier.oom.SelectSql;
import org.sosostudio.dbunifier.oom.UpdateKeyValueClause;
import org.sosostudio.dbunifier.oom.UpdateSql;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.IoUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BaseTester extends TestCase {

	protected String tableName = "SYS_TEST";

	protected String columnName = "TEST_VALUE";

	protected String sequenceName = "SEQ_NAME";

	protected DbUnifier unifier;

	public BaseTester() {
		unifier = new DbUnifier();
	}

	private byte[] getFile(String filename) {
		InputStream is = BaseTester.class.getResourceAsStream(filename);
		try {
			return IoUtil.convertStream(is);
		} catch (IOException e) {
			throw new DbUnifierException(e);
		} finally {
			IoUtil.closeInputStream(is);
		}
	}

	public void testString(String typeName) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			String value = new String("abcdefghij");
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addStringClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql);
			Row row = rowSet.getRow(0);
			String value2 = row.getString(columnName);
			assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql()
					.setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addStringClause(
									columnName, null))
					.setConditionClause(
							new ConditionClause(LogicalOp.AND).addStringClause(
									columnName, RelationOp.EQUAL, value));
			count = unifier.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql);
			row = rowSet.getRow(0);
			String value3 = row.getString(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	private void testNumber(String typeName, BigDecimal value) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addNumberClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql);
			Row row = rowSet.getRow(0);
			BigDecimal value2 = row.getNumber(columnName);
			assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql()
					.setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addNumberClause(
									columnName, null))
					.setConditionClause(
							new ConditionClause(LogicalOp.AND).addNumberClause(
									columnName, RelationOp.EQUAL, value));
			count = unifier.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql);
			row = rowSet.getRow(0);
			BigDecimal value3 = row.getNumber(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	public void testInteger(String typeName) {
		testNumber(typeName, new BigDecimal("123456789"));
	}

	public void testSmallInteger(String typeName) {
		testNumber(typeName, new BigDecimal("1"));
	}

	public void testHighPrecisionDecimal(String typeName) {
		testNumber(typeName, new BigDecimal("1234567.8901234567"));
	}

	public void testDecimal(String typeName) {
		testNumber(typeName, new BigDecimal("1234567.89"));
	}

	public void testSmallDecimal(String typeName) {
		testNumber(typeName, new BigDecimal("1.23"));
	}

	public void testMoney(String typeName) {
		testNumber(typeName, new BigDecimal("12.3456"));
	}

	private void testTimestamp(String typeName, String format) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			Timestamp value = new Timestamp(System.currentTimeMillis());
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addTimestampClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql);
			Row row = rowSet.getRow(0);
			Timestamp value2 = row.getTimestamp(columnName);
			assertEquals(sdf.format(value), sdf.format(value2));
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addTimestampClause(
									columnName, null));
			count = unifier.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql);
			row = rowSet.getRow(0);
			Timestamp value3 = row.getTimestamp(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	public void testTimestamp(String typeName) {
		testTimestamp(typeName, "yyyy-MM-dd HH:mm:ss");
	}

	public void testDate(String typeName) {
		testTimestamp(typeName, "yyyy-MM-dd");
	}

	public void testTime(String typeName) {
		testTimestamp(typeName, "HH:mm:ss");
	}

	private void testClob(String typeName, String value) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addClobClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql, true);
			Row row = rowSet.getRow(0);
			String value2 = row.getClob(columnName);
			assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addClobClause(
									columnName, null));
			count = unifier.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql, true);
			row = rowSet.getRow(0);
			String value3 = row.getClob(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	public void testClob(String typeName) {
		try {
			testClob(typeName, new String(getFile("test.txt"), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
		}
	}

	public void testSmallClob(String typeName) {
		testClob(typeName, "abcdefghijklmnopqrstuvwxyz1234567890");
	}

	private void testBlob(String typeName, byte[] value) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addBlobClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql, true);
			Row row = rowSet.getRow(0);
			byte[] value2 = row.getBlob(columnName);
			for (int i = 0; i < value.length; i++) {
				assertEquals(value[i], value2[i]);
			}
			assertEquals(value.length, value2.length);
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addBlobClause(
									columnName, null));
			count = unifier.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql, true);
			row = rowSet.getRow(0);
			byte[] value3 = row.getBlob(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	public void testBlob(String typeName) {
		testBlob(typeName, getFile("test.bin"));
	}

	public void testSmallBlob(String typeName) {
		testBlob(typeName, new byte[200]);
	}

	@Test
	public void testGetTableList() {
		try {
			unifier.executeOtherSql("create table " + tableName + " ("
					+ columnName + " varchar(50))", null);
			boolean success = false;
			String tableName2 = null;
			List<Table> tableList = unifier.getTableList();
			for (Table table : tableList) {
				if (tableName.equalsIgnoreCase(table.getName())) {
					success = true;
					tableName2 = table.getName();
				}
			}
			assertTrue(success);
			Table table2 = unifier.getTable(tableName2);
			assertEquals(tableName2, table2.getName());
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	@Test
	public void testGetTable() {
		try {
			unifier.createTable(new Table()
					.setName(tableName)
					.addColumn(
							new Column("ST_VALUE", ColumnType.STRING,
									false, true))
					.addColumn(
							new Column("NM_VALUE", ColumnType.NUMBER,
									false, false))
					.addColumn(
							new Column("DT_VALUE", ColumnType.TIMESTAMP,
									false, false))
					.addColumn(
							new Column("CL_VALUE", ColumnType.CLOB, false,
									false))
					.addColumn(
							new Column("BL_VALUE", ColumnType.BLOB, false,
									false)));
			Table table = unifier.getTable(tableName);
			List<Column> columnList = table.getColumnList();
			assertEquals(columnList.size(), 5);
			Column column = columnList.get(0);
			assertEquals(column.isPrimaryKey(), true);
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	@Test
	public void testGetPage() {
		try {
			unifier.createTable(new Table().setName(tableName).addColumn(
					new Column(columnName, ColumnType.STRING, true, true)));
			for (int i = 0; i < 10; i++) {
				unifier.executeInsertSql(new InsertSql()
						.setTableName(tableName).setInsertKeyValueClause(
								new InsertKeyValueClause().addStringClause(
										columnName, i + "")));
			}
			SelectSql selectSql = new SelectSql()
					.setTableName(tableName)
					.setColumns("*")
					.setOrderByClause(
							new OrderByClause().addOrder(columnName,
									Direction.ASC));
			{
				RowSet rowSet = unifier.executeSelectSql(selectSql, 3, 2);
				assertEquals(rowSet.size(), 3);
				assertEquals(rowSet.getPageSize(), 3);
				assertEquals(rowSet.getPageNumber(), 2);
				assertEquals(rowSet.getTotalRowCount(), 10);
				assertEquals(rowSet.getTotalPageCount(), 4);
				Row row = rowSet.getRow(0);
				String value = row.getString(1);
				assertEquals(value, "3");
			}
			{
				RowSet rowSet = unifier.executeSelectSql(selectSql, 3, 4);
				assertEquals(rowSet.size(), 1);
				assertEquals(rowSet.getPageSize(), 3);
				assertEquals(rowSet.getPageNumber(), 4);
				assertEquals(rowSet.getTotalRowCount(), 10);
				assertEquals(rowSet.getTotalPageCount(), 4);
				Row row = rowSet.getRow(0);
				String value = row.getString(1);
				assertEquals(value, "9");
			}
			{
				RowSet rowSet = unifier.executeSelectSql(selectSql, 5, 2);
				assertEquals(rowSet.size(), 5);
				assertEquals(rowSet.getPageSize(), 5);
				assertEquals(rowSet.getPageNumber(), 2);
				assertEquals(rowSet.getTotalRowCount(), 10);
				assertEquals(rowSet.getTotalPageCount(), 2);
				Row row = rowSet.getRow(0);
				String value = row.getString(1);
				assertEquals(value, "5");
			}
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	@Test
	public void testGetSequenceNextValue() {
		long value = unifier.getSequenceNextValue(sequenceName);
		value++;
		long value2 = unifier.getSequenceNextValue(sequenceName);
		assertEquals(value, value2);
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
	public void testCharBinary() {
		String typeName = "char(50) binary";
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
	public void testDouble() {
		String typeName = "double";
		testDecimal(typeName);
	}

	@Test
	public void testDoublePrecision() {
		String typeName = "double precision";
		testHighPrecisionDecimal(typeName);
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
	public void testLongBlob() {
		String typeName = "longblob";
		testBlob(typeName);
	}

	@Test
	public void testLong() {
		String typeName = "long";
		testClob(typeName);
	}

	@Test
	public void testLongRaw() {
		String typeName = "long raw";
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
	public void testMoney() {
		String typeName = "money";
		testMoney(typeName);
	}

	@Test
	public void testNationalChar() {
		String typeName = "national char(10)";
		testString(typeName);
	}

	@Test
	public void testNationalCharacter() {
		String typeName = "national character(10)";
		testString(typeName);
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
	public void testNvarchar() {
		String typeName = "nvarchar(50)";
		testString(typeName);
	}

	@Test
	public void testNvarchar2() {
		String typeName = "nvarchar2(50)";
		testString(typeName);
	}

	@Test
	public void testRaw() {
		String typeName = "raw(200)";
		testSmallBlob(typeName);
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
	public void testVarBinary() {
		String typeName = "varbinary(200)";
		testSmallBlob(typeName);
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
