package org.sosostudio.dbking.sample;

import java.io.IOException;

import org.sosostudio.dbking.Column;
import org.sosostudio.dbking.ColumnType;
import org.sosostudio.dbking.DbKing;
import org.sosostudio.dbking.Table;
import org.sosostudio.dbking.autocode.DaoGenerator;

import freemarker.template.TemplateException;

public class AutoCodeSample {

	private static String tableName = "SYS_TEST";

	public static void main(String[] args) throws TemplateException,
			IOException {
		DbKing dbKing = new DbKing();
		try {
			dbKing.createTable(new Table(tableName)
					.addColumn(
							new Column("ST_VALUE", ColumnType.STRING)
									.setPrimaryKey(true))
					.addColumn(new Column("NM_VALUE", ColumnType.NUMBER))
					.addColumn(new Column("DT_VALUE", ColumnType.TIMESTAMP)));
			DaoGenerator.main(new String[] { "test",
					"org.sosostudio.dbking.sample.dao", tableName });
		} finally {
			dbKing.execute("drop table " + tableName);
		}
	}

}
