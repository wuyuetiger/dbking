package org.sosostudio.dbunifier.feature;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class OracleFeature extends DbFeature {

	@Override
	public String getDatabaseSchema(DatabaseMetaData databaseMetaData)
			throws SQLException {
		return databaseMetaData.getUserName();
	}

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append("select * from ( select row_.*, rownum rownum_ from ( ")
				.append(mainSubSql).append(orderBySubSql)
				.append(" ) row_ where rownum <= ").append(endPos)
				.append(") where rownum_ > ").append(startPos);
		return sb.toString();
	}

}
