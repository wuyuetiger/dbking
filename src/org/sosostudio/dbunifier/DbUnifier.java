package org.sosostudio.dbunifier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.sosostudio.dbunifier.feature.DbFeature;
import org.sosostudio.dbunifier.oom.ConditionClause;
import org.sosostudio.dbunifier.oom.DeleteSql;
import org.sosostudio.dbunifier.oom.InsertKeyValueClause;
import org.sosostudio.dbunifier.oom.InsertSql;
import org.sosostudio.dbunifier.oom.LogicalOp;
import org.sosostudio.dbunifier.oom.OrderByClause;
import org.sosostudio.dbunifier.oom.RelationOp;
import org.sosostudio.dbunifier.oom.SelectSql;
import org.sosostudio.dbunifier.oom.UpdateKeyValueClause;
import org.sosostudio.dbunifier.oom.UpdateSql;

public class DbUnifier {

	private static Boolean existsSequenceTable = false;

	private DbConfig dbConfig;

	private Connection con;

	public DbUnifier(DbConfig dbConfig) {
		this.dbConfig = dbConfig;
	}

	public DbUnifier(Connection con) {
		this.con = con;
	}

	public void closeConnection(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		}
	}

	public void closeStatement(Statement statement) {
		try {
			if (statement != null) {
				statement.close();
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		}
	}

	public void closeResultSet(ResultSet resultSet) {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		}
	}

	public String getDatabaseName() {
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		try {
			DatabaseMetaData databaseMetaData = con.getMetaData();
			return databaseMetaData.getDatabaseProductName();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public List<Table> getTableList() {
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		try {
			List<Table> tableList = new ArrayList<Table>();
			ResultSet tableResultSet = null;
			try {
				DatabaseMetaData dmd = con.getMetaData();
				DbFeature dbFeature = DbFeature.getInstance(dmd);
				String schema = dbFeature.getDatabaseSchema(dmd);
				String[] types = { "TABLE" };
				tableResultSet = dmd.getTables(null, schema, "%", types);
				while (tableResultSet.next()) {
					String tableName = tableResultSet.getString("TABLE_NAME");
					tableName = tableName.toUpperCase();
					if (tableName.startsWith("BIN$")) {
						continue;
					}
					Set<String> primaryKeySet = new HashSet<String>();
					ResultSet primaryKeyResultSet = null;
					try {
						primaryKeyResultSet = dmd.getPrimaryKeys(null, schema,
								tableName);
						while (primaryKeyResultSet.next()) {
							String columnName = primaryKeyResultSet
									.getString("COLUMN_NAME");
							columnName = columnName.toUpperCase();
							primaryKeySet.add(columnName);
						}
					} finally {
						closeResultSet(primaryKeyResultSet);
					}
					List<Column> columnList = new ArrayList<Column>();
					ResultSet columnResultSet = null;
					try {
						columnResultSet = dmd.getColumns(null, schema,
								tableName, "%");
						while (columnResultSet.next()) {
							String columnName = columnResultSet
									.getString("COLUMN_NAME");
							columnName = columnName.toUpperCase();
							short dataType = columnResultSet
									.getShort("DATA_TYPE");
							int size = columnResultSet.getInt("COLUMN_SIZE");
							int nullable = columnResultSet.getInt("NULLABLE");
							Column column = new Column();
							column.setName(columnName);
							column.setType(getColumnType(dataType));
							column.setSize(size);
							column.setNullable(nullable != ResultSetMetaData.columnNoNulls);
							column.setIsPrimaryKey(primaryKeySet
									.contains(columnName));
							columnList.add(column);
						}
					} finally {
						closeResultSet(columnResultSet);
					}
					Table table = new Table();
					table.setName(tableName);
					table.addColumnList(columnList);
					tableList.add(table);
				}
			} finally {
				closeResultSet(tableResultSet);
			}
			return tableList;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public Table getTable(String tableName) {
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		try {
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			String schema = dbFeature.getDatabaseSchema(dmd);
			tableName = tableName.toUpperCase();
			Set<String> primaryKeySet = new HashSet<String>();
			ResultSet primaryKeyResultSet = null;
			try {
				primaryKeyResultSet = dmd.getPrimaryKeys(null, schema,
						tableName);
				while (primaryKeyResultSet.next()) {
					String columnName = primaryKeyResultSet
							.getString("COLUMN_NAME");
					columnName = columnName.toUpperCase();
					primaryKeySet.add(columnName);
				}
			} finally {
				closeResultSet(primaryKeyResultSet);
			}
			List<Column> columnList = new ArrayList<Column>();
			ResultSet columnResultSet = null;
			try {
				columnResultSet = dmd.getColumns(null, schema, tableName, "%");
				boolean hasColumns = false;
				while (columnResultSet.next()) {
					hasColumns = true;
					String columnName = columnResultSet
							.getString("COLUMN_NAME");
					columnName = columnName.toUpperCase();
					short dataType = columnResultSet.getShort("DATA_TYPE");
					int size = columnResultSet.getInt("COLUMN_SIZE");
					int nullable = columnResultSet.getInt("NULLABLE");
					Column column = new Column();
					column.setName(columnName);
					column.setType(getColumnType(dataType));
					column.setSize(size);
					column.setNullable(nullable != ResultSetMetaData.columnNoNulls);
					column.setIsPrimaryKey(primaryKeySet.contains(columnName));
					columnList.add(column);
				}
				if (!hasColumns) {
					return null;
				}
			} finally {
				closeResultSet(columnResultSet);
			}
			Table table = new Table();
			table.setName(tableName);
			table.addColumnList(columnList);
			return table;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public List<Table> getViewList() {
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		try {
			List<Table> viewList = new ArrayList<Table>();
			ResultSet viewResultSet = null;
			try {
				DatabaseMetaData dmd = con.getMetaData();
				DbFeature dbFeature = DbFeature.getInstance(dmd);
				String schema = dbFeature.getDatabaseSchema(dmd);
				String[] types = { "VIEW" };
				viewResultSet = dmd.getTables(null, schema, "%", types);
				while (viewResultSet.next()) {
					String viewName = viewResultSet.getString("TABLE_NAME");
					viewName = viewName.toUpperCase();
					List<Column> columnList = new ArrayList<Column>();
					ResultSet columnResultSet = null;
					try {
						columnResultSet = dmd.getColumns(null, schema,
								viewName, "%");
						while (columnResultSet.next()) {
							String columnName = columnResultSet
									.getString("COLUMN_NAME");
							columnName = columnName.toUpperCase();
							short dataType = columnResultSet
									.getShort("DATA_TYPE");
							int size = columnResultSet.getInt("COLUMN_SIZE");
							Column column = new Column();
							column.setName(columnName);
							column.setType(getColumnType(dataType));
							column.setSize(size);
							column.setNullable(true);
							column.setIsPrimaryKey(false);
							columnList.add(column);
						}
					} finally {
						closeResultSet(columnResultSet);
					}
					Table view = new Table();
					view.setName(viewName);
					view.addColumnList(columnList);
					viewList.add(view);
				}
			} finally {
				closeResultSet(viewResultSet);
			}
			return viewList;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public Table getView(String viewName) {
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		try {
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			String schema = dbFeature.getDatabaseSchema(dmd);
			viewName = viewName.toUpperCase();
			List<Column> columnList = new ArrayList<Column>();
			ResultSet columnResultSet = null;
			try {
				columnResultSet = dmd.getColumns(null, schema, viewName, "%");
				while (columnResultSet.next()) {
					String columnName = columnResultSet
							.getString("COLUMN_NAME");
					columnName = columnName.toUpperCase();
					short dataType = columnResultSet.getShort("DATA_TYPE");
					int size = columnResultSet.getInt("COLUMN_SIZE");
					Column column = new Column();
					column.setName(columnName);
					column.setType(getColumnType(dataType));
					column.setSize(size);
					column.setNullable(true);
					column.setIsPrimaryKey(false);
					columnList.add(column);
				}
			} finally {
				closeResultSet(columnResultSet);
			}
			Table view = new Table();
			view.setName(viewName);
			view.addColumnList(columnList);
			return view;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public void createTable(Table table) {
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		try {
			DatabaseMetaData dmd = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(dmd);
			StringBuilder sb = new StringBuilder();
			StringBuilder sbPk = new StringBuilder();
			sb.append("create table ").append(table.getName()).append("(");
			List<Column> columnList = table.getColumnList();
			for (int i = 0; i < columnList.size(); i++) {
				Column column = (Column) columnList.get(i);
				if (column.getIsPrimaryKey()) {
					if (sbPk.length() > 0) {
						sbPk.append(", ");
					}
					sbPk.append(column.getName());
				}
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(column.getName()).append(" ");
				String type = column.getType();
				if (Column.TYPE_STRING.equals(type)) {
					sb.append(dbFeature.getStringDbType(column.getSize()));
				} else if (Column.TYPE_NUMBER.equals(type)) {
					sb.append(dbFeature.getNumberDbType());
				} else if (Column.TYPE_TIMESTAMP.equals(type)) {
					sb.append(dbFeature.getTimestampDbType());
				} else if (Column.TYPE_CLOB.equals(type)) {
					sb.append(dbFeature.getClobDbType());
				} else if (Column.TYPE_BLOB.equals(type)) {
					sb.append(dbFeature.getBlobDbType());
				}
				if (!column.getNullable()) {
					sb.append(" not null");
				}
			}
			if (sbPk.length() > 0) {
				sb.append(", constraint ").append("PK_" + table.getName())
						.append(" primary key (").append(sbPk).append(")");
			}
			sb.append(")");
			String sql = sb.toString();
			executeOtherSql(sql, null);
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				closeConnection(con);
			}
		}

	}

	public RowSet executeSelectSql(String sql, Values values,
			boolean containsLob, int pageSize, int pageNumber) {
		if (values == null) {
			values = new Values();
		}
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
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
			int totalRowCount;
			String countSql = "select count(*) from (" + mainSubSql + ") t";
			printSql(countSql, values);
			PreparedStatement ps2 = null;
			ResultSet rs2 = null;
			try {
				ps2 = con.prepareStatement(countSql);
				setColumnValue(ps2, 1, values);
				rs2 = ps2.executeQuery();
				if (rs2.next()) {
					totalRowCount = rs2.getBigDecimal(1).intValue();
				} else {
					throw new DbUnifierException("no resultset");
				}
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			} finally {
				closeResultSet(rs2);
				closeStatement(ps2);
			}
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
				printSql(sql, values);
				ps = con.prepareStatement(sql,
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
			} else {
				printSql(paginationSql, values);
				ps = con.prepareStatement(paginationSql);
			}
			setColumnValue(ps, 1, values);
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
					String type = getColumnType(dataType);
					Object value = null;
					if (Column.TYPE_STRING.equals(type)) {
						value = rs.getString(i + 1);
					} else if (Column.TYPE_NUMBER.equals(type)) {
						value = rs.getBigDecimal(i + 1);
					} else if (Column.TYPE_TIMESTAMP.equals(type)) {
						value = rs.getTimestamp(i + 1);
					} else if (Column.TYPE_CLOB.equals(type)) {
						if (containsLob) {
							Reader reader = rs.getCharacterStream(i + 1);
							if (reader != null) {
								StringWriter sw = new StringWriter();
								char[] buffer = new char[2048];
								int charsRead;
								try {
									while ((charsRead = reader.read(buffer, 0,
											1024)) != -1) {
										sw.write(buffer, 0, charsRead);
									}
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									try {
										reader.close();
									} catch (IOException e) {
										throw new DbUnifierException(e);
									}
								}
								value = sw.getBuffer().toString();
							}
						}
					} else if (Column.TYPE_BLOB.equals(type)) {
						if (containsLob) {
							InputStream is = rs.getBinaryStream(i + 1);
							if (is != null) {
								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								byte[] buffer = new byte[2048];
								int bytesRead;
								try {
									while ((bytesRead = is
											.read(buffer, 0, 1024)) != -1) {
										baos.write(buffer, 0, bytesRead);
									}
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									try {
										is.close();
									} catch (IOException e) {
										throw new DbUnifierException(e);
									}
								}
								value = baos.toByteArray();
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
			closeResultSet(rs);
			closeStatement(ps);
			if (this.con == null) {
				closeConnection(con);
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
		printSql(sql, values);
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// query
			RowSet rowSet = new RowSet();
			ps = con.prepareStatement(sql);
			setColumnValue(ps, 1, values);
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
					String type = getColumnType(dataType);
					Object value = null;
					if (Column.TYPE_STRING.equals(type)) {
						value = rs.getString(i + 1);
					} else if (Column.TYPE_NUMBER.equals(type)) {
						value = rs.getBigDecimal(i + 1);
					} else if (Column.TYPE_TIMESTAMP.equals(type)) {
						value = rs.getTimestamp(i + 1);
					} else if (Column.TYPE_CLOB.equals(type)) {
						if (containsLob) {
							Reader reader = rs.getCharacterStream(i + 1);
							if (reader != null) {
								StringWriter sw = new StringWriter();
								char[] buffer = new char[2048];
								int charsRead;
								try {
									while ((charsRead = reader.read(buffer, 0,
											1024)) != -1) {
										sw.write(buffer, 0, charsRead);
									}
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									try {
										reader.close();
									} catch (IOException e) {
										throw new DbUnifierException(e);
									}
								}
								value = sw.getBuffer().toString();
							}
						}
					} else if (Column.TYPE_BLOB.equals(type)) {
						if (containsLob) {
							InputStream is = rs.getBinaryStream(i + 1);
							if (is != null) {
								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								byte[] buffer = new byte[2048];
								int bytesRead;
								try {
									while ((bytesRead = is
											.read(buffer, 0, 1024)) != -1) {
										baos.write(buffer, 0, bytesRead);
									}
								} catch (IOException e) {
									throw new DbUnifierException(e);
								} finally {
									try {
										is.close();
									} catch (IOException e) {
										throw new DbUnifierException(e);
									}
								}
								value = baos.toByteArray();
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
			closeResultSet(rs);
			closeStatement(ps);
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public RowSet executeSelectSql(String sql, Values values) {
		return executeSelectSql(sql, values, false);
	}

	public RowSet executeSelectSql(String sql) {
		Values values = new Values();
		return executeSelectSql(sql, values, false);
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
		printSql(sql, values);
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement(sql);
			setColumnValue(ps, 1, values);
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			closeStatement(ps);
			if (this.con == null) {
				closeConnection(con);
			}
		}
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
		printSql(sql, values);
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(sql);
			setColumnValue(ps, 1, values);
			rs = ps.executeQuery();
			while (rs.next()) {
				Reader reader = rs.getCharacterStream(1);
				if (reader != null) {
					char[] buffer = new char[2048];
					int charsRead;
					try {
						while ((charsRead = reader.read(buffer, 0, 1024)) != -1) {
							writer.write(buffer, 0, charsRead);
						}
					} catch (IOException e) {
						throw new DbUnifierException(e);
					} finally {
						try {
							reader.close();
						} catch (IOException e) {
							throw new DbUnifierException(e);
						}
					}
				}
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public void setSingleClob(String sql, Values values, Reader reader, int size) {
		if (values == null) {
			values = new Values();
		}
		printSql(sql, values);
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement(sql);
			if (reader == null) {
				ps.setCharacterStream(1, null, 0);
			} else {
				ps.setCharacterStream(1, reader, size);
			}
			setColumnValue(ps, 2, values);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			closeStatement(ps);
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public void getSingleBlob(String sql, Values values, OutputStream os) {
		if (values == null) {
			values = new Values();
		}
		printSql(sql, values);
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(sql);
			setColumnValue(ps, 1, values);
			rs = ps.executeQuery();
			while (rs.next()) {
				InputStream is = rs.getBinaryStream(1);
				if (is != null) {
					byte[] buffer = new byte[2048];
					int bytesRead;
					try {
						while ((bytesRead = is.read(buffer, 0, 1024)) != -1) {
							os.write(buffer, 0, bytesRead);
						}
					} catch (IOException e) {
						throw new DbUnifierException(e);
					} finally {
						try {
							is.close();
						} catch (IOException e) {
							throw new DbUnifierException(e);
						}
					}
				}
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public void setSingleBlob(String sql, Values values, InputStream is,
			int size) {
		if (values == null) {
			values = new Values();
		}
		printSql(sql, values);
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement(sql);
			if (is == null) {
				ps.setBinaryStream(1, null, 0);
			} else {
				ps.setBinaryStream(1, is, size);
			}
			setColumnValue(ps, 2, values);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			closeStatement(ps);
			if (this.con == null) {
				closeConnection(con);
			}
		}
	}

	public long getSequenceNextValue(String sequenceName) {
		synchronized (existsSequenceTable) {
			if (!existsSequenceTable) {
				Table table = getTable("SYS_SEQ");
				if (table == null) {
					table = new Table("SYS_SEQ").addColumn(
							new Column("ST_SEQ_NAME", Column.TYPE_STRING, 50,
									false, true)).addColumn(
							new Column("NM_SEQ_VALUE", Column.TYPE_NUMBER, -1,
									false, false));
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
			BigDecimal nextSeqValue = new BigDecimal(seqValue.longValue() + 1
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
				nextSeqValue = new BigDecimal(seqValue.longValue() + 1 + "");
			}
			return nextSeqValue.longValue();
		}
	}

	private String getColumnType(int dataType) {
		if (dataType == Types.VARCHAR) {
			return Column.TYPE_STRING;
		} else if (dataType == Types.LONGVARCHAR) {
			return Column.TYPE_STRING;
		} else if (dataType == Types.NVARCHAR) {
			return Column.TYPE_STRING;
		} else if (dataType == Types.CHAR) {
			return Column.TYPE_STRING;
		} else if (dataType == Types.BIT) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.INTEGER) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.BIGINT) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.SMALLINT) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.TINYINT) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.FLOAT) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.DOUBLE) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.DECIMAL) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.NUMERIC) {
			return Column.TYPE_NUMBER;
		} else if (dataType == Types.TIMESTAMP) {
			return Column.TYPE_TIMESTAMP;
		} else if (dataType == Types.DATE) {
			return Column.TYPE_TIMESTAMP;
		} else if (dataType == Types.TIME) {
			return Column.TYPE_TIMESTAMP;
		} else if (dataType == Types.BLOB) {
			return Column.TYPE_BLOB;
		} else if (dataType == Types.BINARY) {
			return Column.TYPE_BLOB;
		} else if (dataType == Types.VARBINARY) {
			return Column.TYPE_BLOB;
		} else if (dataType == Types.LONGVARBINARY) {
			return Column.TYPE_BLOB;
		} else if (dataType == Types.CLOB) {
			return Column.TYPE_CLOB;
		} else {
			return Column.TYPE_UNKNOWN;
		}
	}

	private void setColumnValue(PreparedStatement ps, int startPos,
			Values values) throws SQLException {
		List<Object> valueList = values.getValueList();
		for (int i = 0; i < valueList.size(); i++) {
			int pos = startPos + i;
			Object value = valueList.get(i);
			if (value instanceof NullValue) {
				NullValue nullValue = (NullValue) value;
				String type = nullValue.getType();
				if (Column.TYPE_STRING.equals(type)) {
					ps.setString(pos, null);
				} else if (Column.TYPE_NUMBER.equals(type)) {
					ps.setBigDecimal(pos, null);
				} else if (Column.TYPE_TIMESTAMP.equals(type)) {
					ps.setTimestamp(pos, null);
				} else if (Column.TYPE_CLOB.equals(type)) {
					ps.setCharacterStream(pos, null, 0);
				} else if (Column.TYPE_BLOB.equals(type)) {
					ps.setBinaryStream(pos, null, 0);
				}
			} else if (value instanceof String) {
				ps.setString(pos, (String) value);
			} else if (value instanceof BigDecimal) {
				ps.setBigDecimal(pos, (BigDecimal) value);
			} else if (value instanceof Timestamp) {
				ps.setTimestamp(pos, (Timestamp) value);
			} else if (value instanceof char[]) {
				char[] chars = (char[]) value;
				Reader reader = new StringReader(new String(chars));
				ps.setCharacterStream(pos, reader, chars.length);
			} else if (value instanceof byte[]) {
				byte[] bytes = (byte[]) value;
				InputStream is = new ByteArrayInputStream(bytes);
				ps.setBinaryStream(pos, is, bytes.length);
			}
		}
	}

	private void printSql(String sql, Values values) {
		StringBuilder sb = new StringBuilder();
		sb.append("\n").append(sql).append("\n");
		List<Object> valueList = values.getValueList();
		for (int i = 0; i < valueList.size(); i++) {
			Object value = valueList.get(i);
			sb.append(i + 1).append(".[");
			if (value instanceof NullValue) {
				sb.append("<NULL>");
			} else if (value instanceof String) {
				sb.append(value);
			} else if (value instanceof BigDecimal) {
				sb.append(value);
			} else if (value instanceof Timestamp) {
				sb.append(value);
			} else if (value instanceof char[]) {
				sb.append("<CLOB>");
			} else if (value instanceof byte[]) {
				sb.append("<BLOB>");
			}
			sb.append("]\n");
		}
		System.out.println(sb.toString());
	}

}
