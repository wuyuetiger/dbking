package org.sosostudio.dbking.sample;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.sosostudio.dbking.Column;
import org.sosostudio.dbking.ColumnType;
import org.sosostudio.dbking.DbUnifier;
import org.sosostudio.dbking.Table;
import org.sosostudio.dbking.autocode.DaoGenerator;

import freemarker.template.TemplateException;

public class AutoCodeSample {

	private static String tableName = "SYS_TEST";

	public static void main(String[] args) throws UnsupportedEncodingException,
			TemplateException, IOException {
		DbUnifier unifier = new DbUnifier();
		try {
			unifier.createTable(new Table(tableName)
					.addColumn(
							new Column("ST_VALUE", ColumnType.STRING)
									.setPrimaryKey(true))
					.addColumn(new Column("NM_VALUE", ColumnType.NUMBER))
					.addColumn(new Column("DT_VALUE", ColumnType.TIMESTAMP)));
			DaoGenerator.main(new String[] { "test",
					"org.sosostudio.dbunifier.sample.dao", tableName });
		} finally {
			unifier.executeOtherSql("drop table " + tableName);
		}
	}

}
