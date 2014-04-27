import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.sosostudio.dbunifier.Column;
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

public abstract class BaseTester {

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
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			String value = new String("abcdefghij");
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addStringClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			Assert.assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql);
			Row row = rowSet.getRow(0);
			String value2 = row.getString(columnName);
			Assert.assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql()
					.setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addStringClause(
									columnName, null))
					.setConditionClause(
							new ConditionClause(LogicalOp.AND).addStringClause(
									columnName, RelationOp.EQUAL, value));
			count = unifier.executeUpdateSql(updateSql);
			Assert.assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql);
			row = rowSet.getRow(0);
			String value3 = row.getString(columnName);
			Assert.assertNull(value3);
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
			Assert.assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql);
			Row row = rowSet.getRow(0);
			BigDecimal value2 = row.getNumber(columnName);
			Assert.assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql()
					.setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addNumberClause(
									columnName, null))
					.setConditionClause(
							new ConditionClause(LogicalOp.AND).addNumberClause(
									columnName, RelationOp.EQUAL, value));
			count = unifier.executeUpdateSql(updateSql);
			Assert.assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql);
			row = rowSet.getRow(0);
			BigDecimal value3 = row.getNumber(columnName);
			Assert.assertNull(value3);
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	public void testInteger(String typeName) {
		testNumber(typeName, new BigDecimal("123456789"));
	}

	public void testDecimal(String typeName) {
		testNumber(typeName, new BigDecimal("1234567.89"));
	}

	public void testSmallDecimal(String typeName) {
		testNumber(typeName, new BigDecimal("1.23"));
	}

	public void testTimestamp(String typeName) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			Timestamp value = Timestamp.valueOf(new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss").format(new Date()));
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addTimestampClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			Assert.assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql);
			Row row = rowSet.getRow(0);
			Timestamp value2 = row.getTimestamp(columnName);
			Assert.assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql()
					.setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addTimestampClause(
									columnName, null))
					.setConditionClause(
							new ConditionClause(LogicalOp.AND)
									.addTimestampClause(columnName,
											RelationOp.EQUAL, value));
			count = unifier.executeUpdateSql(updateSql);
			Assert.assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql);
			row = rowSet.getRow(0);
			Timestamp value3 = row.getTimestamp(columnName);
			Assert.assertNull(value3);
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	public void testClob(String typeName) {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " " + typeName + ")", null);
		try {
			String value = new String(getFile("test.txt"));
			InsertSql insertSql = new InsertSql().setTableName(tableName)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addClobClause(
									columnName, value));
			int count = unifier.executeInsertSql(insertSql);
			Assert.assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql, true);
			Row row = rowSet.getRow(0);
			String value2 = row.getClob(columnName);
			Assert.assertEquals(value, value2);
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addClobClause(
									columnName, null));
			count = unifier.executeUpdateSql(updateSql);
			Assert.assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql, true);
			row = rowSet.getRow(0);
			String value3 = row.getClob(columnName);
			Assert.assertNull(value3);
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
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
			Assert.assertEquals(count, 1);
			SelectSql selectSql = new SelectSql().setTableName(tableName)
					.setColumns(columnName);
			RowSet rowSet = unifier.executeSelectSql(selectSql, true);
			Row row = rowSet.getRow(0);
			byte[] value2 = row.getBlob(columnName);
			for (int i = 0; i < value.length; i++) {
				Assert.assertEquals(value[i], value2[i]);
			}
			Assert.assertEquals(value.length, value2.length);
			UpdateSql updateSql = new UpdateSql().setTableName(tableName)
					.setUpdateKeyValueClause(
							new UpdateKeyValueClause().addBlobClause(
									columnName, null));
			count = unifier.executeUpdateSql(updateSql);
			Assert.assertEquals(count, 1);
			rowSet = unifier.executeSelectSql(selectSql, true);
			row = rowSet.getRow(0);
			byte[] value3 = row.getBlob(columnName);
			Assert.assertNull(value3);
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	public void testStandardBlob(String typeName) {
		testBlob(typeName, getFile("test.bin"));
	}

	public void testSmallBlob(String typeName) {
		testBlob(typeName, new byte[2000]);
	}

	@Test
	public void testGetTableInfo() {
		unifier.executeOtherSql("create table " + tableName + " (" + columnName
				+ " varchar(50))", null);
		try {
			boolean success = false;
			List<Table> tableList = unifier.getTableList();
			for (Table table : tableList) {
				if (tableName.equals(table.getName())) {
					success = true;
				}
			}
			Assert.assertTrue(success);
			Table table = unifier.getTable(tableName);
			Assert.assertEquals(tableName, table.getName());
			System.out.println(table);
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

	@Test
	public void testPageSelect() {
		unifier.createTable(new Table().setName(tableName).addColumn(
				new Column(columnName, Column.TYPE_STRING, 50, true, true)));
		try {
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
				Assert.assertEquals(rowSet.size(), 3);
				Assert.assertEquals(rowSet.getPageSize(), 3);
				Assert.assertEquals(rowSet.getPageNumber(), 2);
				Assert.assertEquals(rowSet.getTotalRowCount(), 10);
				Assert.assertEquals(rowSet.getTotalPageCount(), 4);
				Row row = rowSet.getRow(0);
				String value = row.getString(1);
				Assert.assertEquals(value, "3");
			}
			{
				RowSet rowSet = unifier.executeSelectSql(selectSql, 3, 4);
				Assert.assertEquals(rowSet.size(), 1);
				Assert.assertEquals(rowSet.getPageSize(), 3);
				Assert.assertEquals(rowSet.getPageNumber(), 4);
				Assert.assertEquals(rowSet.getTotalRowCount(), 10);
				Assert.assertEquals(rowSet.getTotalPageCount(), 4);
				Row row = rowSet.getRow(0);
				String value = row.getString(1);
				Assert.assertEquals(value, "9");
			}
			{
				RowSet rowSet = unifier.executeSelectSql(selectSql, 5, 2);
				Assert.assertEquals(rowSet.size(), 5);
				Assert.assertEquals(rowSet.getPageSize(), 5);
				Assert.assertEquals(rowSet.getPageNumber(), 2);
				Assert.assertEquals(rowSet.getTotalRowCount(), 10);
				Assert.assertEquals(rowSet.getTotalPageCount(), 2);
				Row row = rowSet.getRow(0);
				String value = row.getString(1);
				Assert.assertEquals(value, "5");
				System.out.println(rowSet);
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
		Assert.assertEquals(value, value2);
	}

}
