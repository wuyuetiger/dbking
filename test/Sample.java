import org.sosostudio.dbunifier.DbConfig;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Row;
import org.sosostudio.dbunifier.RowSet;
import org.sosostudio.dbunifier.oom.Direction;
import org.sosostudio.dbunifier.oom.OrderByClause;
import org.sosostudio.dbunifier.oom.SelectSql;

public class Sample {

	public static void main(String[] args) {
		DbConfig config = new DbConfig("com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306/test", "root", "password");
		DbUnifier unifier = new DbUnifier(config);
		SelectSql selectSql = new SelectSql()
				.setTableName("CS_USER")
				.setColumns("*")
				.setOrderByClause(
						new OrderByClause().addOrder("USERNAME", Direction.ASC));
		RowSet rowSet = unifier.executeSelectSql(selectSql, 3, 2);
		for (int i = 0; i < rowSet.size(); i++) {
			Row row = rowSet.getRow(i);
			String username = row.getString("USERNAME");
			System.out.println(username);
		}
	}

}
