package org.sosostudio.dbunifier.pipe;

import java.util.List;

import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Table;

public class Tester2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DbUnifier unifier = new DbUnifier();
		List<Table> tableList = unifier.getTableList();
		for(Table table : tableList) {
			//String sql = "create table " + table.getName() + " as select * from rk." + table.getName() + " where 1=0;";
			String sql = "delete from " + table.getName() + ";";
			System.out.println(sql);
		}
	}

}
