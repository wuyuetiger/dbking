package org.sosostudio.dbunifier.feature;

public class PostgreSqlFeature extends DbFeature {

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append(mainSubSql).append(orderBySubSql).append(" limit ")
				.append(endPos - startPos).append(" offset ").append(startPos);
		return sb.toString();
	}

	@Override
	public String getNumberDbType() {
		return "decimal";
	}

	@Override
	public String getTimestampDbType() {
		return "timestamp";
	}

	@Override
	public String getClobDbType() {
		return "text";
	}

	@Override
	public String getBlobDbType() {
		return "bytea";
	}

}
