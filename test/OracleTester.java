import java.math.BigDecimal;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sosostudio.dbunifier.DbConfig;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Row;
import org.sosostudio.dbunifier.RowSet;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.oom.InsertKeyValueClause;
import org.sosostudio.dbunifier.oom.InsertSql;
import org.sosostudio.dbunifier.oom.SelectSql;

public class OracleTester {

	DbUnifier unifier;

	public OracleTester() {
		DbConfig config = new DbConfig("oracle.jdbc.driver.OracleDriver",
				"jdbc:oracle:thin:@localhost:1521:orcl", "system", "55780029");
		unifier = new DbUnifier(config);
	}

	@Before
	public void setUp() {
		StringBuilder sb = new StringBuilder();
		sb.append("create table SYS_TEST (").append("INTV int").append(")");
		String sql = sb.toString();
		unifier.executeOtherSql(sql, null);
	}

	@After
	public void tearDown() {
		String sql = "drop table SYS_TEST ";
		unifier.executeOtherSql(sql, null);
	}

	@Test
	public void test1() {
		boolean success = false;
		List<Table> tableList = unifier.getTableList();
		for (Table table : tableList) {
			if ("SYS_TEST".equals(table.getName())) {
				success = true;
			}
		}
		Assert.assertTrue(success);
		Table table = unifier.getTable("SYS_TEST");
		Assert.assertEquals("SYS_TEST", table.getName());
		System.out.println(table);
	}

	@Test
	public void test2() {
		BigDecimal intv = new BigDecimal("1234567890");
		InsertSql insertSql = new InsertSql().setTableName("SYS_TEST")
				.setInsertKeyValueClause(
						new InsertKeyValueClause()
								.addNumberClause("INTV", intv));
		int count = unifier.executeInsertSql(insertSql);
		Assert.assertEquals(count, 1);
		SelectSql selectSql = new SelectSql().setTableName("SYS_TEST")
				.setColumns("INTV");
		RowSet rowSet = unifier.executeSelectSql(selectSql);
		Row row = rowSet.getRow(0);
		BigDecimal intv2 = row.getNumber("INTV");
		Assert.assertEquals(intv, intv2);
	}

}
