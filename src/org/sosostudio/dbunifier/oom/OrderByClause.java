package org.sosostudio.dbunifier.oom;

public class OrderByClause {

	private StringBuilder sb = new StringBuilder();

	public OrderByClause addOrder(String columnName, Direction direction) {
		if (sb.length() > 0) {
			sb.append(", ");
		}
		sb.append(columnName).append(" ");
		if (direction == Direction.ASC) {
			sb.append("asc");
		} else if (direction == Direction.DESC) {
			sb.append("desc");
		}
		return this;
	}

}
