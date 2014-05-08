import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.sosostudio.dbunifier.Column;
import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.autocode.DaoGenerator;

import freemarker.template.TemplateException;

public class AutoCodeTester {

	private static String tableName = "SYS_TEST";

	public static void main(String[] args) throws UnsupportedEncodingException,
			TemplateException, IOException {
		DbUnifier unifier = new DbUnifier();
		try {
			unifier.createTable(new Table()
					.setName(tableName)
					.addColumn(
							new Column("ST_VALUE", ColumnType.TYPE_STRING,
									false, true))
					.addColumn(
							new Column("NM_VALUE", ColumnType.TYPE_NUMBER,
									false, false))
					.addColumn(
							new Column("DT_VALUE", ColumnType.TYPE_TIMESTAMP,
									false, false)));
			DaoGenerator.main(new String[] { "test", "test.dbunifier",
					tableName });
		} finally {
			unifier.executeOtherSql("drop table " + tableName, null);
		}
	}

}
