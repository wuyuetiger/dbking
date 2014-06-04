package org.sosostudio.dbunifier.sample;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.sosostudio.dbunifier.Column;
import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.autocode.DaoGenerator;

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
