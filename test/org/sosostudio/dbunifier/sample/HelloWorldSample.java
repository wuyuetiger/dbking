package org.sosostudio.dbunifier.sample;

import java.util.List;

import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Row;
import org.sosostudio.dbunifier.RowSet;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.oom.Direction;
import org.sosostudio.dbunifier.oom.OrderByClause;
import org.sosostudio.dbunifier.oom.SelectSql;

public class HelloWorldSample {

	public static void main(String[] args) {
		DbUnifier unifier = new DbUnifier();
		unifier.getSequenceNextValue("test");
		List<Table> tableList = unifier.getTableList();
		for (Table table : tableList) {
			System.out.println(table);
		}
		SelectSql selectSql = new SelectSql()
				.setTableName(DbUnifier.SEQ_TABLE_NAME)
				.setColumns("*")
				.setOrderByClause(
						new OrderByClause().addOrder(
								DbUnifier.SEQ_NAME_COLUMN_NAME, Direction.ASC));
		RowSet rowSet = unifier.executeSelectSql(selectSql, 3, 2);
		for (Row row : rowSet) {
			String seqName = row.getString(DbUnifier.SEQ_NAME_COLUMN_NAME);
			System.out.println(seqName);
		}
	}

}
