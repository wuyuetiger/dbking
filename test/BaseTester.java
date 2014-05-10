import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.sosostudio.dbunifier.Column;
import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.DbUnifierException;
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

public abstract class BaseTester extends TestCase {

	protected String tableName = "SYS_TEST";

	protected String columnName = "TEST_VALUE";

	protected String sequenceName = "SEQ_NAME";

	protected DbUnifier unifier;

	public BaseTester() {
		init();
	}

	public abstract void init();

	private byte[] getFile(String filename) {
		InputStream is = BaseTester.class.getResourceAsStream(filename);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buffer = new byte[2048];
		int bytesRead;
		try {
			while ((bytesRead = is.read(buffer, 0, 1024)) != -1) {
				baos.write(buffer, 0, bytesRead);
			}
		} catch (IOException e) {
			throw new DbUnifierException(e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
		}
		return baos.toByteArray();
	}

	public void testString(String typeName) {
		try {
			unifier.executeOtherSql("create table " + tableName + " ("
					+ columnName + " " + typeName + ")", null);
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
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	private void testNumber(String typeName, BigDecimal value) {
		try {
			unifier.executeOtherSql("create table " + tableName + " ("
					+ columnName + " " + typeName + ")", null);
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
		try {
			unifier.executeOtherSql("create table " + tableName + " ("
					+ columnName + " " + typeName + ")", null);
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
		try {
			unifier.executeOtherSql("create table " + tableName + " ("
					+ columnName + " " + typeName + ")", null);
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
		try {
			unifier.executeOtherSql("create table " + tableName + " ("
					+ columnName + " " + typeName + ")", null);
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
	public void testGetTableInfo() {
		try {
			unifier.executeOtherSql("create table " + tableName + " ("
					+ columnName + " varchar(50))", null);
			boolean success = false;
			List<Table> tableList = unifier.getTableList();
			for (Table table : tableList) {
				if (tableName.equals(table.getName())) {
					success = true;
				}
			}
			assertTrue(success);
			Table table = unifier.getTable(tableName);
			assertEquals(tableName, table.getName());
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	@Test
	public void testPageSelect() {
		try {
			unifier.createTable(new Table().setName(tableName).addColumn(
					new Column(columnName, ColumnType.TYPE_STRING, true, true)));
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

}
