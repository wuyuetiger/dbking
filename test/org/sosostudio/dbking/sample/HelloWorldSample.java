package org.sosostudio.dbking.sample;

import java.util.List;

import org.sosostudio.dbking.DbKing;
import org.sosostudio.dbking.Row;
import org.sosostudio.dbking.RowSet;
import org.sosostudio.dbking.Table;
import org.sosostudio.dbking.oom.Direction;
import org.sosostudio.dbking.oom.OrderByClause;
import org.sosostudio.dbking.oom.SelectSql;

public class HelloWorldSample {

	public static void main(String[] args) {
		DbKing dbKing = new DbKing();
		dbKing.getSequenceNextValue("test");
		List<Table> tableList = dbKing.getTableList();
		for (Table table : tableList) {
			System.out.println(table);
		}
		SelectSql selectSql = new SelectSql()
				.setTableName(DbKing.SEQ_TABLE_NAME)
				.setColumns("*")
				.setOrderByClause(
						new OrderByClause().addOrder(
								DbKing.SEQ_NAME_COLUMN_NAME, Direction.ASC));
		RowSet rowSet = dbKing.executeSelectSql(selectSql, 3, 2);
		for (Row row : rowSet) {
			String seqName = row.getString(DbKing.SEQ_NAME_COLUMN_NAME);
			System.out.println(seqName);
		}
	}

}
