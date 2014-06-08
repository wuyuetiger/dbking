package org.sosostudio.dbking.test;

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
import org.sosostudio.dbking.Column;
import org.sosostudio.dbking.ColumnType;
import org.sosostudio.dbking.DbKing;
import org.sosostudio.dbking.Row;
import org.sosostudio.dbking.RowSet;
import org.sosostudio.dbking.Table;
import org.sosostudio.dbking.exception.DbKingException;
import org.sosostudio.dbking.oom.ConditionClause;
import org.sosostudio.dbking.oom.Direction;
import org.sosostudio.dbking.oom.InsertKeyValueClause;
import org.sosostudio.dbking.oom.InsertSql;
import org.sosostudio.dbking.oom.LogicalOp;
import org.sosostudio.dbking.oom.OrderByClause;
import org.sosostudio.dbking.oom.RelationOp;
import org.sosostudio.dbking.oom.SelectSql;
import org.sosostudio.dbking.oom.UpdateKeyValueClause;
import org.sosostudio.dbking.oom.UpdateSql;
import org.sosostudio.dbking.util.IoUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BaseTester extends TestCase {

	protected String tableName = "SYS_TEST";

	protected String columnName = "TEST_VALUE";

	protected String sequenceName = "SEQ_NAME";

	protected DbKing dbKing;

	public BaseTester() {
		dbKing = new DbKing();
	}

	protected void createTable(String typeName) {
		dbKing.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")");
	}

	private byte[] getFile(String filename) {
		InputStream is = BaseTester.class.getResourceAsStream(filename);
		try {
			return IoUtil.convertStream(is);
		} catch (IOException e) {
			throw new DbKingException(e);
		} finally {
			IoUtil.closeInputStream(is);
		}
	}

	private void testString(String typeName, String value) {
		createTable(typeName);
		try {
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addStringClause(
									columnName, value));
			int count = dbKing.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = dbKing.executeSelectSql(selectSql);
			Row row = rowSet.get(0);
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
			count = dbKing.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = dbKing.executeSelectSql(selectSql);
			row = rowSet.get(0);
			String value3 = row.getString(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			dbKing.dropTable(tableName);
		}
	}

	public void testString(String typeName) {
		testString(typeName, "abcdefghij");
	}

	public void testChineseString4(String typeName) {
		testString(typeName, "一二三四");
	}

	public void testChineseString6(String typeName) {
		testString(typeName, "一二三四五六");
	}

	public void testChineseString12(String typeName) {
		testString(typeName, "一二三四五六七八九十百千");
	}

	private void testNumber(String typeName, BigDecimal value) {
		createTable(typeName);
		try {
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addNumberClause(
									columnName, value));
			int count = dbKing.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = dbKing.executeSelectSql(selectSql);
			Row row = rowSet.get(0);
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
			count = dbKing.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = dbKing.executeSelectSql(selectSql);
			row = rowSet.get(0);
			BigDecimal value3 = row.getNumber(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			dbKing.dropTable(tableName);
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
		createTable(typeName);
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			Timestamp value = new Timestamp(System.currentTimeMillis());
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addTimestampClause(
									columnName, value));
			int count = dbKing.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = dbKing.executeSelectSql(selectSql);
			Row row = rowSet.get(0);
			Timestamp value2 = row.getTimestamp(columnName);
			assertEquals(sdf.format(value), sdf.format(value2));
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addTimestampClause(
									columnName, null));
			count = dbKing.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = dbKing.executeSelectSql(selectSql);
			row = rowSet.get(0);
			Timestamp value3 = row.getTimestamp(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			dbKing.dropTable(tableName);
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
		createTable(typeName);
		try {
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addClobClause(
									columnName, value));
			int count = dbKing.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = dbKing.executeSelectSql(selectSql, true);
			Row row = rowSet.get(0);
			String value2 = row.getClob(columnName);
			assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addClobClause(
									columnName, null));
			count = dbKing.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = dbKing.executeSelectSql(selectSql, true);
			row = rowSet.get(0);
			String value3 = row.getClob(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			dbKing.dropTable(tableName);
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
		createTable(typeName);
		try {
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addBlobClause(
									columnName, value));
			int count = dbKing.executeInsertSql(insertSql);
			assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = dbKing.executeSelectSql(selectSql, true);
			Row row = rowSet.get(0);
			byte[] value2 = row.getBlob(columnName);
			for (int i = 0; i < value.length; i++) {
				assertEquals(value[i], value2[i]);
			}
			assertEquals(value.length, value2.length);
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addBlobClause(
									columnName, null));
			count = dbKing.executeUpdateSql(updateSql);
			assertEquals(count, 1);
			rowSet = dbKing.executeSelectSql(selectSql, true);
			row = rowSet.get(0);
			byte[] value3 = row.getBlob(columnName);
			assertNull(value3);
		} catch (Exception e) {
			fail(e.toString());
		} finally {
			dbKing.dropTable(tableName);
		}
	}

	public void testBlob(String typeName) {
		testBlob(typeName, getFile("test.bin"));
	}

	public void testSmallBlob(String typeName) {
		testBlob(typeName, new byte[200]);
	}

	@Test
	public void testFuncGetTableList() {
		dbKing.createTable(new Table(tableName).addColumn(new Column(
				columnName, ColumnType.STRING).setSize(50)));
		try {
			boolean success = false;
			String tableName2 = null;
			List<Table> tableList = dbKing.getTableList();
			for (Table table : tableList) {
				if (tableName.equalsIgnoreCase(table.getName())) {
					success = true;
					tableName2 = table.getName();
				}
			}
			assertTrue(success);
			Table table2 = dbKing.getTable(tableName2);
			assertEquals(tableName2, table2.getName());
		} finally {
			dbKing.dropTable(tableName);
		}
	}

	@Test
	public void testFuncGetTable() {
		dbKing.createTable(new Table(tableName)
				.addColumn(
						new Column("ST_VALUE", ColumnType.STRING)
								.setPrimaryKey(true))
				.addColumn(new Column("NM_VALUE", ColumnType.NUMBER))
				.addColumn(new Column("DT_VALUE", ColumnType.TIMESTAMP))
				.addColumn(new Column("CL_VALUE", ColumnType.CLOB))
				.addColumn(new Column("BL_VALUE", ColumnType.BLOB)));
		try {
			Table table = dbKing.getTable(tableName);
			List<Column> columnList = table.getColumnList();
			assertEquals(columnList.size(), 5);
			Column column = columnList.get(0);
			assertEquals(column.isPrimaryKey(), true);
		} finally {
			dbKing.dropTable(tableName);
		}
	}

	@Test
	public void testFuncGetPage() {
		dbKing.createTable(new Table(tableName).addColumn(new Column(
				columnName, ColumnType.STRING).setPrimaryKey(true)));
		try {
			for (int i = 0; i < 10; i++) {
				dbKing.executeInsertSql(new InsertSql().setTableName(tableName)
						.setInsertKeyValueClause(
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
				RowSet rowSet = dbKing.executeSelectSql(selectSql, 3, 2);
				assertEquals(rowSet.size(), 3);
				assertEquals(rowSet.getPageSize(), 3);
				assertEquals(rowSet.getPageNumber(), 2);
				assertEquals(rowSet.getTotalRowCount(), 10);
				assertEquals(rowSet.getTotalPageCount(), 4);
				Row row = rowSet.get(0);
				String value = row.getString(1);
				assertEquals(value, "3");
			}
			{
				RowSet rowSet = dbKing.executeSelectSql(selectSql, 3, 4);
				assertEquals(rowSet.size(), 1);
				assertEquals(rowSet.getPageSize(), 3);
				assertEquals(rowSet.getPageNumber(), 4);
				assertEquals(rowSet.getTotalRowCount(), 10);
				assertEquals(rowSet.getTotalPageCount(), 4);
				Row row = rowSet.get(0);
				String value = row.getString(1);
				assertEquals(value, "9");
			}
			{
				RowSet rowSet = dbKing.executeSelectSql(selectSql, 5, 2);
				assertEquals(rowSet.size(), 5);
				assertEquals(rowSet.getPageSize(), 5);
				assertEquals(rowSet.getPageNumber(), 2);
				assertEquals(rowSet.getTotalRowCount(), 10);
				assertEquals(rowSet.getTotalPageCount(), 2);
				Row row = rowSet.get(0);
				String value = row.getString(1);
				assertEquals(value, "5");
			}
		} finally {
			dbKing.dropTable(tableName);
		}
	}

	@Test
	public void testFuncGetSequenceNextValue() {
		long value = dbKing.getSequenceNextValue(sequenceName);
		value++;
		long value2 = dbKing.getSequenceNextValue(sequenceName);
		assertEquals(value, value2);
	}

	@Test
	public void testTypeBigint() {
		String typeName = "bigint";
		testInteger(typeName);
	}

	@Test
	public void testTypeBinary() {
		String typeName = "binary";
		testBlob(typeName);
	}

	@Test
	public void testTypeBit() {
		String typeName = "bit";
		testSmallInteger(typeName);
	}

	@Test
	public void testTypeBlob() {
		String typeName = "blob";
		testBlob(typeName);
	}

	@Test
	public void testTypeBool() {
		String typeName = "bool";
		testSmallInteger(typeName);
	}

	@Test
	public void testTypeBoolean() {
		String typeName = "boolean";
		testSmallInteger(typeName);
	}

	@Test
	public void testTypeBytea() {
		String typeName = "bytea";
		testBlob(typeName);
	}

	@Test
	public void testTypeChar() {
		String typeName = "char(10)";
		testString(typeName);
	}

	@Test
	public void testTypeCharVarying() {
		String typeName = "char varying(50)";
		testString(typeName);
	}

	@Test
	public void testTypeCharacter() {
		String typeName = "character(10)";
		testString(typeName);
	}

	@Test
	public void testTypeCharacterVarying() {
		String typeName = "character varying(50)";
		testString(typeName);
	}

	@Test
	public void testTypeClob() {
		String typeName = "clob";
		testClob(typeName);
	}

	@Test
	public void testTypeDate() {
		String typeName = "date";
		testDate(typeName);
	}

	@Test
	public void testTypeDatetime() {
		String typeName = "datetime";
		testTimestamp(typeName);
	}

	@Test
	public void testTypeDec() {
		String typeName = "dec(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testTypeDecimal() {
		String typeName = "decimal(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testTypeDouble() {
		String typeName = "double";
		testDecimal(typeName);
	}

	@Test
	public void testTypeDoublePrecision() {
		String typeName = "double precision";
		testHighPrecisionDecimal(typeName);
	}

	@Test
	public void testTypeFloat() {
		String typeName = "float";
		testSmallDecimal(typeName);
	}

	@Test
	public void testTypeImage() {
		String typeName = "image";
		testBlob(typeName);
	}

	@Test
	public void testTypeInt() {
		String typeName = "int";
		testInteger(typeName);
	}

	@Test
	public void testTypeInteger() {
		String typeName = "integer";
		testInteger(typeName);
	}

	@Test
	public void testTypeLong() {
		String typeName = "long";
		testClob(typeName);
	}

	@Test
	public void testTypeLongRaw() {
		String typeName = "long raw";
		testBlob(typeName);
	}

	@Test
	public void testTypeLongVarchar() {
		String typeName = "long varchar";
		testClob(typeName);
	}

	@Test
	public void testTypeLongblob() {
		String typeName = "longblob";
		testBlob(typeName);
	}

	@Test
	public void testTypeLongtext() {
		String typeName = "longtext";
		testClob(typeName);
	}

	@Test
	public void testTypeMoney() {
		String typeName = "money";
		testMoney(typeName);
	}

	@Test
	public void testTypeNationalChar() {
		String typeName = "national char(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNationalCharVarying() {
		String typeName = "national char varying(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNationalCharacter() {
		String typeName = "national character(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNationalCharacterVarying() {
		String typeName = "national character varying(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNchar() {
		String typeName = "nchar(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNcharVarying() {
		String typeName = "nchar varying(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNclob() {
		String typeName = "nclob";
		testClob(typeName);
	}

	@Test
	public void testTypeNumber() {
		String typeName = "number(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testTypeNumeric() {
		String typeName = "numeric(10,2)";
		testDecimal(typeName);
	}

	@Test
	public void testTypeNvarchar() {
		String typeName = "nvarchar(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeNvarchar2() {
		String typeName = "nvarchar2(12)";
		testChineseString12(typeName);
	}

	@Test
	public void testTypeRaw() {
		String typeName = "raw(200)";
		testSmallBlob(typeName);
	}

	@Test
	public void testTypeReal() {
		String typeName = "real";
		testDecimal(typeName);
	}

	@Test
	public void testTypeSmallint() {
		String typeName = "smallint";
		testSmallInteger(typeName);
	}

	@Test
	public void testTypeText() {
		String typeName = "text";
		testClob(typeName);
	}

	@Test
	public void testTypeTime() {
		String typeName = "time";
		testTime(typeName);
	}

	@Test
	public void testTypeTimestamp() {
		String typeName = "timestamp";
		testTimestamp(typeName);
	}

	@Test
	public void testTypeTinyint() {
		String typeName = "tinyint";
		testSmallInteger(typeName);
	}

	@Test
	public void testTypeVarbinary() {
		String typeName = "varbinary(200)";
		testSmallBlob(typeName);
	}

	@Test
	public void testTypeVarchar() {
		String typeName = "varchar(50)";
		testString(typeName);
	}

	@Test
	public void testTypeVarchar2() {
		String typeName = "varchar2(50)";
		testString(typeName);
	}

	@Test
	public void testEncVarcharByUTF8() {
		String typeName = "varchar(12)";
		testChineseString4(typeName);
	}

	@Test
	public void testEncVarcharByGBK() {
		String typeName = "varchar(12)";
		testChineseString6(typeName);
	}

	@Test
	public void testEncVarcharByUnicode() {
		String typeName = "varchar(12)";
		testChineseString12(typeName);
	}

}
