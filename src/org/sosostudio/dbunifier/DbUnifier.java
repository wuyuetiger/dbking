/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2014 YU YUE, SOSO STUDIO, wuyuetiger@gmail.com
 *
 * License: GNU Lesser General Public License (LGPL)
 * 
 * Source code availability:
 *  https://github.com/wuyuetiger/db-unifier
 *  https://code.csdn.net/tigeryu/db-unifier
 *  https://git.oschina.net/db-unifier/db-unifier
 */

package org.sosostudio.dbunifier;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.sosostudio.dbunifier.config.XmlConfig;
import org.sosostudio.dbunifier.dbsource.ConnectionDbSource;
import org.sosostudio.dbunifier.dbsource.DbSource;
import org.sosostudio.dbunifier.feature.DbFeature;
import org.sosostudio.dbunifier.oom.ConditionClause;
import org.sosostudio.dbunifier.oom.DeleteSql;
import org.sosostudio.dbunifier.oom.ExtraClause;
import org.sosostudio.dbunifier.oom.InsertKeyValueClause;
import org.sosostudio.dbunifier.oom.InsertSql;
import org.sosostudio.dbunifier.oom.LogicalOp;
import org.sosostudio.dbunifier.oom.OrderByClause;
import org.sosostudio.dbunifier.oom.RelationOp;
import org.sosostudio.dbunifier.oom.SelectSql;
import org.sosostudio.dbunifier.oom.UpdateKeyValueClause;
import org.sosostudio.dbunifier.oom.UpdateSql;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.DbUtil;
import org.sosostudio.dbunifier.util.IoUtil;

public class DbUnifier {

	private static Boolean existsSequenceTable = false;

	private DbSource dbSource;

	public DbUnifier() {
		DbSource dbSource = XmlConfig.getDbSource("");
		this.dbSource = dbSource;
	}

	public DbUnifier(String dbSourceName) {
		DbSource dbSource = XmlConfig.getDbSource(dbSourceName);
		this.dbSource = dbSource;
	}

	public DbUnifier(DbSource dbSource) {
		this.dbSource = dbSource;
	}

	public DbUnifier(Connection con, Encoding encoding) {
		this.dbSource = new ConnectionDbSource(con, encoding);
	}

	public DbUnifier(Connection con) {
		this.dbSource = new ConnectionDbSource(con);
	}

	public void printDatabaseInfo() {
		Connection con = null;
		try {
			con = dbSource.getConnection();
			DatabaseMetaData dmd = con.getMetaData();
			System.out.println();
			System.out.println("DatabaseProductName: "
					+ dmd.getDatabaseProductName());
			System.out.println("DatabaseProductVersion: "
					+ dmd.getDatabaseProductVersion());
			System.out.println("DatabaseMajorVersion: "
					+ dmd.getDatabaseMajorVersion());
			System.out.println("DatabaseMinorVersion: "
					+ dmd.getDatabaseMinorVersion());
			System.out.println("DriverName: " + dmd.getDriverName());
			System.out.println("DriverMajorVersion: "
					+ dmd.getDriverMajorVersion());
			System.out.println("DriverMinorVersion: "
					+ dmd.getDriverMinorVersion());
			System.out.println("DriverVersion: " + dmd.getDriverVersion());
			System.out
					.println("JDBCMajorVersion: " + dmd.getJDBCMajorVersion());
			System.out
					.println("JDBCMinorVersion: " + dmd.getJDBCMinorVersion());

			System.out.println();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public Encoding getEncoding() {
		Encoding encoding = dbSource.getEncoding();
		if (encoding != null) {
			return encoding;
		}
		Connection con = null;
		try {
			con = dbSource.getConnection();
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			return dbFeature.getEncoding();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	private Column getColumn(DbFeature dbFeature, ResultSet columnRs,
			Set<String> pkSet) throws SQLException {
		String columnName = columnRs.getString("COLUMN_NAME");
		short dataType = columnRs.getShort("DATA_TYPE");
		String dbType = columnRs.getString("TYPE_NAME");
		int size = columnRs.getInt("COLUMN_SIZE");
		int scale = columnRs.getInt("DECIMAL_DIGITS");
		int nullable = columnRs.getInt("NULLABLE");
		Column column = new Column(columnName, DbUtil.getColumnType(dataType));
		column.setNationalString(DbUtil.isNationalString(dbType));
		column.setSize(size);
		column.setPrecision(size);
		column.setScale(scale);
		column.setNullable(nullable != ResultSetMetaData.columnNoNulls);
		if (pkSet != null) {
			column.setPrimaryKey(pkSet.contains(columnName));
		}
		return column;
	}

	private int getDepth(Table table, Set<String> tableNameSet) {
		String tableName = table.getName();
		tableNameSet.add(tableName);
		List<Table> parentTableList = table.getParentTableList();
		int depth = 0;
		for (Table parentTable : parentTableList) {
			String parentTableName = parentTable.getName();
			if (tableNameSet.contains(parentTableName)) {
				continue;
			}
			depth = Math.max(depth, getDepth(parentTable, tableNameSet));
		}
		tableNameSet.remove(tableName);
		return depth + 1;
	}

	public List<Table> getTableList(boolean sortByFk) {
		Connection con = null;
		try {
			con = dbSource.getConnection();
			List<Table> tableList = new ArrayList<Table>();
			Map<String, Table> tableMap = new HashMap<String, Table>();
			ResultSet tableRs = null;
			try {
				DatabaseMetaData dmd = con.getMetaData();
				DbFeature dbFeature = DbFeature.getInstance(dmd);
				String schema = dbFeature.getDatabaseSchema(dmd);
				String[] types = { "TABLE" };
				tableRs = dmd.getTables(null, schema, "%", types);
				while (tableRs.next()) {
					String tableName = tableRs.getString("TABLE_NAME");
					if (tableName.startsWith("BIN$")) {
						continue;
					}
					Set<String> pkSet = new HashSet<String>();
					ResultSet pkRs = null;
					try {
						pkRs = dmd.getPrimaryKeys(null, schema, tableName);
						while (pkRs.next()) {
							String columnName = pkRs.getString("COLUMN_NAME");
							pkSet.add(columnName);
						}
					} finally {
						DbUtil.closeResultSet(pkRs);
					}
					List<Column> columnList = new ArrayList<Column>();
					ResultSet columnRs = null;
					try {
						columnRs = dmd.getColumns(null, schema, tableName, "%");
						while (columnRs.next()) {
							Column column = getColumn(dbFeature, columnRs,
									pkSet);
							columnList.add(column);
						}
					} finally {
						DbUtil.closeResultSet(columnRs);
					}
					Table table = new Table(tableName);
					table.addColumnList(columnList);
					tableList.add(table);
					tableMap.put(tableName, table);
				}
				if (sortByFk) {
					for (Table table : tableList) {
						String tableName = table.getName();
						ResultSet fkRs = null;
						try {
							fkRs = dmd.getImportedKeys(null, schema, tableName);
							while (fkRs.next()) {
								String parentTableName = fkRs
										.getString("PKTABLE_NAME");
								Table parentTable = tableMap
										.get(parentTableName);
								table.addParentTable(parentTable);
							}
						} finally {
							DbUtil.closeResultSet(fkRs);
						}
					}
					for (Table table : tableList) {
						int depth = getDepth(table, new HashSet<String>());
						table.setDepth(depth);
					}
					Collections.sort(tableList);
				}
			} finally {
				DbUtil.closeResultSet(tableRs);
			}
			for (Table table : tableList) {
				table.setName(table.getName().toUpperCase());
				for (Column column : table.getColumnList()) {
					column.setName(column.getName().toUpperCase());
				}
			}
			return tableList;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public List<Table> getTableList() {
		return getTableList(false);
	}

	public Table getTable(String tableName) {
		Connection con = null;
		try {
			con = dbSource.getConnection();
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			String schema = dbFeature.getDatabaseSchema(dmd);
			Set<String> pkSet = new HashSet<String>();
			ResultSet pkRs = null;
			try {
				pkRs = dmd.getPrimaryKeys(null, schema, tableName);
				while (pkRs.next()) {
					String columnName = pkRs.getString("COLUMN_NAME");
					pkSet.add(columnName);
				}
			} finally {
				DbUtil.closeResultSet(pkRs);
			}
			List<Column> columnList = new ArrayList<Column>();
			ResultSet columnRs = null;
			try {
				columnRs = dmd.getColumns(null, schema, tableName, "%");
				while (columnRs.next()) {
					Column column = getColumn(dbFeature, columnRs, pkSet);
					columnList.add(column);
				}
			} finally {
				DbUtil.closeResultSet(columnRs);
			}
			if (columnList.size() > 0) {
				Table table = new Table(tableName.toUpperCase());
				table.addColumnList(columnList);
				for (Column column : table.getColumnList()) {
					column.setName(column.getName().toUpperCase());
				}
				return table;
			} else {
				return null;
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public List<Table> getViewList() {
		Connection con = null;
		try {
			con = dbSource.getConnection();
			List<Table> viewList = new ArrayList<Table>();
			ResultSet viewRs = null;
			try {
				DatabaseMetaData dmd = con.getMetaData();
				DbFeature dbFeature = DbFeature.getInstance(dmd);
				String schema = dbFeature.getDatabaseSchema(dmd);
				String[] types = { "VIEW" };
				viewRs = dmd.getTables(null, schema, "%", types);
				while (viewRs.next()) {
					String viewName = viewRs.getString("TABLE_NAME");
					List<Column> columnList = new ArrayList<Column>();
					ResultSet columnRs = null;
					try {
						columnRs = dmd.getColumns(null, schema, viewName, "%");
						while (columnRs.next()) {
							Column column = getColumn(dbFeature, columnRs, null);
							columnList.add(column);
						}
					} finally {
						DbUtil.closeResultSet(columnRs);
					}
					Table view = new Table(viewName);
					view.setTable(false);
					view.addColumnList(columnList);
					viewList.add(view);
				}
			} finally {
				DbUtil.closeResultSet(viewRs);
			}
			for (Table view : viewList) {
				view.setName(view.getName().toUpperCase());
				for (Column column : view.getColumnList()) {
					column.setName(column.getName().toUpperCase());
				}
			}
			return viewList;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public Table getView(String viewName) {
		Connection con = null;
		try {
			con = dbSource.getConnection();
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			String schema = dbFeature.getDatabaseSchema(dmd);
			List<Column> columnList = new ArrayList<Column>();
			ResultSet columnRs = null;
			try {
				columnRs = dmd.getColumns(null, schema, viewName, "%");
				while (columnRs.next()) {
					Column column = getColumn(dbFeature, columnRs, null);
					columnList.add(column);
				}
			} finally {
				DbUtil.closeResultSet(columnRs);
			}
			if (columnList.size() > 0) {
				Table view = new Table(viewName.toUpperCase());
				view.setTable(false);
				view.addColumnList(columnList);
				for (Column column : view.getColumnList()) {
					column.setName(column.getName().toUpperCase());
				}
				return view;
			} else {
				return null;
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public void createTable(Table table) {
		Connection con = null;
		Statement statement = null;
		try {
			con = dbSource.getConnection();
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			StringBuilder sb = new StringBuilder();
			StringBuilder sbPk = new StringBuilder();
			sb.append("create table ").append(table.getName()).append("(");
			List<Column> columnList = table.getColumnList();
			for (int i = 0; i < columnList.size(); i++) {
				Column column = (Column) columnList.get(i);
				if (column.isPrimaryKey()) {
					if (sbPk.length() > 0) {
						sbPk.append(", ");
					}
					sbPk.append(column.getName());
				}
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(column.getName()).append(" ");
				ColumnType type = column.getType();
				if (type == ColumnType.STRING) {
					if (column.isNationalString()) {
						sb.append(dbFeature.getNStringDbType(column.getSize()));
					} else {
						sb.append(dbFeature.getStringDbType(column.getSize()));
					}
				} else if (type == ColumnType.NUMBER) {
					sb.append(dbFeature.getNumberDbType(column.getPrecision(),
							column.getScale()));
				} else if (type == ColumnType.TIMESTAMP) {
					sb.append(dbFeature.getTimestampDbType());
				} else if (type == ColumnType.CLOB) {
					sb.append(dbFeature.getClobDbType());
				} else if (type == ColumnType.BLOB) {
					sb.append(dbFeature.getBlobDbType());
				}
				if (!column.isNullable() || column.isPrimaryKey()) {
					sb.append(" not null");
				} else if (!dbFeature.allowNullByDefault()) {
					sb.append(" null");
				}
			}
			if (sbPk.length() > 0) {
				String uuid16 = UUID.randomUUID().toString().replace("-", "");
				BigInteger big = new BigInteger(uuid16, 16);
				String uuid36 = big.toString(36);
				sb.append(", constraint ").append("PK_" + uuid36)
						.append(" primary key (").append(sbPk).append(")");
			}
			sb.append(")");
			String sql = sb.toString();
			DbUtil.printSql(sql, null);
			statement = con.createStatement();
			statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeStatement(statement);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}

	}

	public void dropTable(String tableName) {
		String sql = "drop table " + tableName;
		executeOtherSql(sql);
	}

	public RowSet executeSelectSql(String sql, Values values,
			boolean containsLob, int pageSize, int pageNumber) {
		if (values == null) {
			values = new Values();
		}
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = dbSource.getConnection();
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			// split sql into two part
			String mainSubSql = sql;
			String orderBySubSql = "";
			String upperCaseSql = sql.toUpperCase();
			int orderByPos = upperCaseSql.indexOf("ORDER BY");
			while (orderByPos >= 0) {
				String orderByStr = upperCaseSql.substring(orderByPos);
				int count = 0;
				for (int i = 0; i < orderByStr.length(); i++) {
					if (orderByStr.charAt(i) == '\'') {
						count++;
					}
				}
				if (count % 2 == 0) {
					mainSubSql = sql.substring(0, orderByPos);
					orderBySubSql = sql.substring(orderByPos);
					break;
				} else {
					orderByPos = upperCaseSql.indexOf("ORDER BY",
							orderByPos + 1);
				}
			}
			// pagination compute
			int totalRowCount = dbFeature.getTotalRowCount(con, mainSubSql,
					values);
			int totalPageCount = (totalRowCount + pageSize - 1) / pageSize;
			pageNumber = Math.max(1, Math.min(totalPageCount, pageNumber));
			// construct pagination sql
			int startPos = pageSize * (pageNumber - 1);
			int endPos = startPos + pageSize;
			String paginationSql = dbFeature.getPaginationSql(mainSubSql,
					orderBySubSql, startPos, endPos);
			// query
			RowSet rowSet = new RowSet();
			rowSet.setPageNumber(pageNumber);
			rowSet.setPageSize(pageSize);
			rowSet.setTotalRowCount(totalRowCount);
			rowSet.setTotalPageCount(totalPageCount);
			if (paginationSql == null) {
				DbUtil.printSql(sql, values);
				ps = con.prepareStatement(sql,
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
			} else {
				DbUtil.printSql(paginationSql, values);
				ps = con.prepareStatement(paginationSql);
			}
			DbUtil.setColumnValue(ps, 1, values);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			for (int i = 0; i < columnCount; i++) {
				String columnName = rsmd.getColumnLabel(i + 1);
				columnName = columnName.toUpperCase();
				rowSet.addColumnName(columnName);
			}
			if (paginationSql == null) {
				if (!rs.absolute(pageSize * (pageNumber - 1) + 1)) {
					throw new DbUnifierException("abnormal resultset location");
				} else {
					rs.previous();
				}
			}
			while (rs.next()) {
				if (paginationSql == null) {
					if (rs.getRow() > pageSize * pageNumber) {
						break;
					}
				}
				Row row = new Row(rowSet);
				for (int i = 0; i < columnCount; i++) {
					String columnName = rsmd.getColumnLabel(i + 1);
					columnName = columnName.toUpperCase();
					int dataType = rsmd.getColumnType(i + 1);
					ColumnType type = DbUtil.getColumnType(dataType);
					Object value = null;
					if (type == ColumnType.STRING) {
						value = rs.getString(i + 1);
					} else if (type == ColumnType.NUMBER) {
						value = rs.getBigDecimal(i + 1);
					} else if (type == ColumnType.TIMESTAMP) {
						value = rs.getTimestamp(i + 1);
					} else if (type == ColumnType.CLOB) {
						if (containsLob) {
							Reader reader = rs.getCharacterStream(i + 1);
							if (reader != null) {
								try {
									value = IoUtil.convertStream(reader);
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									IoUtil.closeReader(reader);
								}
							}
						}
					} else if (type == ColumnType.BLOB) {
						if (containsLob) {
							InputStream is = rs.getBinaryStream(i + 1);
							if (is != null) {
								try {
									value = IoUtil.convertStream(is);
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									IoUtil.closeInputStream(is);
								}
							}
						}
					} else {
						throw new DbUnifierException("not support data type");
					}
					row.addValue(columnName, value);
				}
				rowSet.addRow(row);
			}
			return rowSet;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeResultSet(rs);
			DbUtil.closeStatement(ps);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public RowSet executeSelectSql(String sql, Values values, int pageSize,
			int pageNumber) {
		return executeSelectSql(sql, values, false, pageSize, pageNumber);
	}

	public RowSet executeSelectSql(String sql, int pageSize, int pageNumber) {
		Values values = new Values();
		return executeSelectSql(sql, values, false, pageSize, pageNumber);
	}

	public RowSet executeSelectSql(String sql, Values values,
			boolean containsLob) {
		if (values == null) {
			values = new Values();
		}
		DbUtil.printSql(sql, values);
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// query
			con = dbSource.getConnection();
			RowSet rowSet = new RowSet();
			ps = con.prepareStatement(sql);
			DbUtil.setColumnValue(ps, 1, values);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			for (int i = 0; i < columnCount; i++) {
				String columnName = rsmd.getColumnLabel(i + 1);
				columnName = columnName.toUpperCase();
				rowSet.addColumnName(columnName);
			}
			while (rs.next()) {
				Row row = new Row(rowSet);
				for (int i = 0; i < columnCount; i++) {
					String columnName = rsmd.getColumnLabel(i + 1);
					columnName = columnName.toUpperCase();
					int dataType = rsmd.getColumnType(i + 1);
					ColumnType type = DbUtil.getColumnType(dataType);
					Object value = null;
					if (type == ColumnType.STRING) {
						value = rs.getString(i + 1);
					} else if (type == ColumnType.NUMBER) {
						value = rs.getBigDecimal(i + 1);
					} else if (type == ColumnType.TIMESTAMP) {
						value = rs.getTimestamp(i + 1);
					} else if (type == ColumnType.CLOB) {
						if (containsLob) {
							Reader reader = rs.getCharacterStream(i + 1);
							if (reader != null) {
								try {
									value = IoUtil.convertStream(reader);
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									IoUtil.closeReader(reader);
								}
							}
						}
					} else if (type == ColumnType.BLOB) {
						if (containsLob) {
							InputStream is = rs.getBinaryStream(i + 1);
							if (is != null) {
								try {
									value = IoUtil.convertStream(is);
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									IoUtil.closeInputStream(is);
								}
							}
						}
					} else {
						throw new DbUnifierException("not support data type");
					}
					row.addValue(columnName, value);
				}
				rowSet.addRow(row);
			}
			rowSet.setTotalRowCount(rowSet.size());
			return rowSet;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeResultSet(rs);
			DbUtil.closeStatement(ps);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public RowSet executeSelectSql(String sql, Values values) {
		return executeSelectSql(sql, values, false);
	}

	public RowSet executeSelectSql(String sql) {
		return executeSelectSql(sql, null);
	}

	public RowSet executeSelectSql(SelectSql selectSql, boolean containsLob,
			int pageSize, int pageNumber) {
		StringBuilder sb = new StringBuilder();
		Values values = new Values();
		sb.append("select ").append(selectSql.getColumns()).append(" from ")
				.append(selectSql.getTableName());
		ConditionClause cc = selectSql.getConditionClause();
		if (cc != null) {
			String clause = cc.getClause();
			if (clause.length() > 0) {
				sb.append(" where ").append(clause);
				values.addValues(cc.getValues());
			}
		}
		ExtraClause ec = selectSql.getExtraClause();
		if (ec != null) {
			sb.append(" ").append(ec.getClause());
			values.addValues(ec.getValues());
		}
		OrderByClause obc = selectSql.getOrderByClause();
		if (obc != null) {
			String clause = obc.getClause();
			if (clause.length() > 0) {
				sb.append(" order by ").append(clause);
			}
		}
		String sql = sb.toString();
		return executeSelectSql(sql, values, containsLob, pageSize, pageNumber);
	}

	public RowSet executeSelectSql(SelectSql selectSql, int pageSize,
			int pageNumber) {
		return executeSelectSql(selectSql, false, pageSize, pageNumber);
	}

	public RowSet executeSelectSql(SelectSql selectSql, boolean containsLob) {
		StringBuilder sb = new StringBuilder();
		Values values = new Values();
		sb.append("select ").append(selectSql.getColumns()).append(" from ")
				.append(selectSql.getTableName());
		ConditionClause cc = selectSql.getConditionClause();
		if (cc != null) {
			String clause = cc.getClause();
			if (clause.length() > 0) {
				sb.append(" where ").append(clause);
				values.addValues(cc.getValues());
			}
		}
		ExtraClause ec = selectSql.getExtraClause();
		if (ec != null) {
			sb.append(" ").append(ec.getClause());
			values.addValues(ec.getValues());
		}
		OrderByClause obc = selectSql.getOrderByClause();
		if (obc != null) {
			String clause = obc.getClause();
			if (clause.length() > 0) {
				sb.append(" order by ").append(clause);
			}
		}
		String sql = sb.toString();
		return executeSelectSql(sql, values, containsLob);
	}

	public RowSet executeSelectSql(SelectSql selectSql) {
		return executeSelectSql(selectSql, false);
	}

	public int executeOtherSql(String sql, Values values) {
		if (values == null) {
			values = new Values();
		}
		DbUtil.printSql(sql, values);
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = dbSource.getConnection();
			ps = con.prepareStatement(sql);
			DbUtil.setColumnValue(ps, 1, values);
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeStatement(ps);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public void executeOtherSql(String sql) {
		executeOtherSql(sql, null);
	}

	public int executeInsertSql(InsertSql insertSql) {
		StringBuilder sb = new StringBuilder();
		Values values = new Values();
		InsertKeyValueClause ikvc = insertSql.getInsertKeyValueClause();
		values.addValues(ikvc.getValues());
		sb.append("insert into ").append(insertSql.getTableName()).append("(")
				.append(ikvc.getKeysClause()).append(") values(")
				.append(ikvc.getValuesClause()).append(")");
		String sql = sb.toString();
		return executeOtherSql(sql, values);
	}

	public int executeUpdateSql(UpdateSql updateSql) {
		StringBuilder sb = new StringBuilder();
		Values values = new Values();
		UpdateKeyValueClause ukvc = updateSql.getUpdateKeyValueClause();
		values.addValues(ukvc.getValues());
		sb.append("update ").append(updateSql.getTableName()).append(" set ")
				.append(ukvc.getClause());
		ConditionClause cc = updateSql.getConditionClause();
		if (cc != null) {
			String clause = cc.getClause();
			if (clause.length() > 0) {
				sb.append(" where ").append(clause);
				values.addValues(cc.getValues());
			}
		}
		String sql = sb.toString();
		return executeOtherSql(sql, values);
	}

	public int executeDeleteSql(DeleteSql deleteSql) {
		StringBuilder sb = new StringBuilder();
		Values values = new Values();
		sb.append("delete from ").append(deleteSql.getTableName());
		ConditionClause cc = deleteSql.getConditionClause();
		if (cc != null) {
			String clause = cc.getClause();
			if (clause.length() > 0) {
				sb.append(" where ").append(clause);
				values.addValues(cc.getValues());
			}
		}
		String sql = sb.toString();
		return executeOtherSql(sql, values);
	}

	public void getSingleClob(String sql, Values values, Writer writer) {
		if (values == null) {
			values = new Values();
		}
		DbUtil.printSql(sql, values);
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = dbSource.getConnection();
			ps = con.prepareStatement(sql);
			DbUtil.setColumnValue(ps, 1, values);
			rs = ps.executeQuery();
			while (rs.next()) {
				Reader reader = rs.getCharacterStream(1);
				if (reader != null) {
					try {
						IoUtil.convertStream(reader, writer);
					} catch (IOException e) {
						throw new DbUnifierException(e);
					} finally {
						IoUtil.closeReader(reader);
					}
				}
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeResultSet(rs);
			DbUtil.closeStatement(ps);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public void setSingleClob(String sql, Values values, Reader reader, int size) {
		if (values == null) {
			values = new Values();
		}
		DbUtil.printSql(sql, values);
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = dbSource.getConnection();
			ps = con.prepareStatement(sql);
			if (reader == null) {
				ps.setCharacterStream(1, null, 0);
			} else {
				ps.setCharacterStream(1, reader, size);
			}
			DbUtil.setColumnValue(ps, 2, values);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeStatement(ps);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public void getSingleBlob(String sql, Values values, OutputStream os) {
		if (values == null) {
			values = new Values();
		}
		DbUtil.printSql(sql, values);
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = dbSource.getConnection();
			ps = con.prepareStatement(sql);
			DbUtil.setColumnValue(ps, 1, values);
			rs = ps.executeQuery();
			while (rs.next()) {
				InputStream is = rs.getBinaryStream(1);
				if (is != null) {
					try {
						IoUtil.convertStream(is, os);
					} catch (IOException e) {
						throw new DbUnifierException(e);
					} finally {
						IoUtil.closeInputStream(is);
					}
				}
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeResultSet(rs);
			DbUtil.closeStatement(ps);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public void setSingleBlob(String sql, Values values, InputStream is,
			int size) {
		if (values == null) {
			values = new Values();
		}
		DbUtil.printSql(sql, values);
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = dbSource.getConnection();
			ps = con.prepareStatement(sql);
			if (is == null) {
				ps.setBinaryStream(1, null, 0);
			} else {
				ps.setBinaryStream(1, is, size);
			}
			DbUtil.setColumnValue(ps, 2, values);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			DbUtil.closeStatement(ps);
			if (!(dbSource instanceof ConnectionDbSource)) {
				DbUtil.closeConnection(con);
			}
		}
	}

	public long getSequenceNextValue(String sequenceName) {
		synchronized (existsSequenceTable) {
			if (!existsSequenceTable) {
				Table table = getTable("SYS_SEQ");
				if (table == null) {
					table = new Table("SYS_SEQ").addColumn(
							new Column("ST_SEQ_NAME", ColumnType.STRING)
									.setPrimaryKey(true)).addColumn(
							new Column("NM_SEQ_VALUE", ColumnType.NUMBER));
					createTable(table);
					existsSequenceTable = true;
				}
			}
		}
		RowSet rs = executeSelectSql(new SelectSql()
				.setTableName("SYS_SEQ")
				.setColumns("NM_SEQ_VALUE")
				.setConditionClause(
						new ConditionClause(LogicalOp.AND).addStringClause(
								"ST_SEQ_NAME", RelationOp.EQUAL, sequenceName)));
		if (rs.size() == 0) {
			executeInsertSql(new InsertSql().setTableName("SYS_SEQ")
					.setInsertKeyValueClause(
							new InsertKeyValueClause().addStringClause(
									"ST_SEQ_NAME", sequenceName)
									.addNumberClause("NM_SEQ_VALUE",
											BigDecimal.ONE)));
			return 1;
		} else {
			Row row = rs.getRow(0);
			BigDecimal seqValue = row.getNumber("NM_SEQ_VALUE");
			BigDecimal nextSeqValue = new BigDecimal((seqValue.longValue() + 1)
					+ "");
			int count = 0;
			while (count == 0) {
				count = executeUpdateSql(new UpdateSql()
						.setTableName("SYS_SEQ")
						.setUpdateKeyValueClause(
								new UpdateKeyValueClause().addNumberClause(
										"NM_SEQ_VALUE", nextSeqValue))
						.setConditionClause(
								new ConditionClause(LogicalOp.AND)
										.addStringClause("ST_SEQ_NAME",
												RelationOp.EQUAL, sequenceName)
										.addNumberClause("NM_SEQ_VALUE",
												RelationOp.EQUAL, seqValue)));
				if (count == 1) {
					break;
				}
				seqValue = nextSeqValue;
				nextSeqValue = new BigDecimal((seqValue.longValue() + 1) + "");
			}
			return nextSeqValue.longValue();
		}
	}

}
