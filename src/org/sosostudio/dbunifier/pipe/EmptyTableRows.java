package org.sosostudio.dbunifier.pipe;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.config.XmlConfig;
import org.sosostudio.dbunifier.dbsource.DbSource;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.DbUtil;

public class EmptyTableRows {

	private static final String CONFIG_NAME = "importer";

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		Connection con = null;
		try {
			DbSource dbSource = XmlConfig.getDbSource(CONFIG_NAME);
			System.out.println("You will operate the following database:");
			System.out.println(dbSource);
			System.out
					.println("Please confirm the database you really want to operate, (Y)es for going on or (N)o for breaking?");
			while (true) {
				int ch = System.in.read();
				if (ch == 'Y' || ch == 'y') {
					break;
				} else if (ch == 'N' || ch == 'n') {
					return;
				}
			}
			DbUnifier unifier = new DbUnifier(con);
			List<Table> tableList = unifier.getTableList(true);
			for (int i = tableList.size() - 1; i >= 0; i--) {
				Table table = tableList.get(i);
				String tableName = table.getName();
				String sql = "delete from " + tableName;
				unifier.executeOtherSql(sql, null);
				System.out.println(tableName + "'s rows have been deleted ");
			}
		} catch (IOException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeConnection(con);
		}
		long end = System.currentTimeMillis();
		System.out.println("It took up " + (end - start) / 1000 + " minutes.");
	}
}
