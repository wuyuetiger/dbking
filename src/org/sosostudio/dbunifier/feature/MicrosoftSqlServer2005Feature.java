package org.sosostudio.dbunifier.feature;

public class MicrosoftSqlServer2005Feature extends
		MicrosoftSqlServer2000Feature {

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		if ("".equals(orderBySubSql)) {
			orderBySubSql = "order by (select 1)";
		}
		sb.append("select * from ( select tmp1_.*, row_number() over (")
				.append(orderBySubSql).append(") rownum_ from ( ")
				.append(mainSubSql).append(" ) tmp1_) tmp2_ where rownum_ > ")
				.append(startPos).append(" and rownum_ <= ").append(endPos);
		return sb.toString();
	}

}
