package org.sosostudio.dbking.sample;

import java.util.List;

import org.sosostudio.dbking.DbUnifier;
import org.sosostudio.dbking.Row;
import org.sosostudio.dbking.RowSet;
import org.sosostudio.dbking.Table;
import org.sosostudio.dbking.oom.Direction;
import org.sosostudio.dbking.oom.OrderByClause;
import org.sosostudio.dbking.oom.SelectSql;

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
