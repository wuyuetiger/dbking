package org.sosostudio.dbking.sample;

import java.math.BigDecimal;
import java.util.List;

import org.sosostudio.dbking.Column;
import org.sosostudio.dbking.ColumnType;
import org.sosostudio.dbking.DbKing;
import org.sosostudio.dbking.Row;
import org.sosostudio.dbking.RowList;
import org.sosostudio.dbking.Table;
import org.sosostudio.dbking.oom.Direction;
import org.sosostudio.dbking.oom.InsertKeyValueClause;
import org.sosostudio.dbking.oom.InsertSql;
import org.sosostudio.dbking.oom.OrderByClause;
import org.sosostudio.dbking.oom.SelectSql;

public class HelloWorldSample {

	private static final String TABLE_NAME = "DBKING_SAMPLE";

	public static void main(String[] args) {
		DbKing dbKing = new DbKing();
		dbKing.getSequenceNextValue("test");
		List<Table> tableList = dbKing.getTableList();
		for (Table table : tableList) {
			System.out.println(table);
		}
		dbKing.createTable(new Table(TABLE_NAME).addColumn(
				new Column("NM_VALUE", ColumnType.NUMBER).setPrimaryKey(true))
				.addColumn(new Column("ST_VALUE", ColumnType.STRING)));
		for (int i = 0; i < 20; i++) {
			dbKing.executeInsertSql(new InsertSql().setTableName(TABLE_NAME)
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addNumberClause(
									"NM_VALUE", new BigDecimal(i + 1))
									.addStringClause("ST_VALUE",
											String.valueOf(i + 1))));
		}
		RowList rowList = dbKing.executeSelectSql(
				new SelectSql()
						.setTableName(TABLE_NAME)
						.setColumns("*")
						.setOrderByClause(
								new OrderByClause().addOrder("NM_VALUE",
										Direction.ASC)), 3, 2);
		for (Row row : rowList) {
			String value = row.getString("ST_VALUE");
			System.out.println(value);
		}
		dbKing.dropTable(TABLE_NAME);
	}

}
