package org.sosostudio.dbunifier.feature;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.sosostudio.dbunifier.Values;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.DbUtil;

public class MySqlFeature extends DbFeature {

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append(mainSubSql).append(orderBySubSql).append(" limit ")
				.append(endPos - startPos).append(" offset ").append(startPos);
		return sb.toString();
	}

	@Override
	public String getTimestampDbType() {
		return "datetime";
	}

	@Override
	public String getClobDbType() {
		return "longclob";
	}

	@Override
	public String getBlobDbType() {
		return "longblob";
	}

	@Override
	public int getTotalRowCount(Connection con, String mainSubSql, Values values) {
		String viewName = "SYS_"
				+ UUID.randomUUID().toString().replaceAll("-", "");
		try {
			{
				String createViewSql = "create view " + viewName + " as "
						+ mainSubSql + "";
				DbUtil.printSql(createViewSql, values);
				PreparedStatement ps = null;
				try {
					ps = con.prepareStatement(createViewSql);
					DbUtil.setColumnValue(ps, 1, values);
					ps.executeUpdate();
				} catch (SQLException e) {
					throw new DbUnifierException(e);
				} finally {
					DbUtil.closeStatement(ps);
				}
			}
			{
				String countSql = "select count(*) from " + viewName;
				DbUtil.printSql(countSql, values);
				PreparedStatement ps = null;
				ResultSet rs = null;
				try {
					ps = con.prepareStatement(countSql);
					DbUtil.setColumnValue(ps, 1, values);
					rs = ps.executeQuery();
					if (rs.next()) {
						return rs.getBigDecimal(1).intValue();
					} else {
						throw new DbUnifierException("no resultset");
					}
				} catch (SQLException e) {
					throw new DbUnifierException(e);
				} finally {
					DbUtil.closeResultSet(rs);
					DbUtil.closeStatement(ps);
				}
			}
		} finally {
			String dropViewSql = "drop view if exists " + viewName;
			DbUtil.printSql(dropViewSql, new Values());
			Statement statement = null;
			try {
				statement = con.createStatement();
				statement.executeUpdate(dropViewSql);
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			} finally {
				DbUtil.closeStatement(statement);
			}
		}
	}

}
