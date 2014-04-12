package org.sosostudio.dbunifier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
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

public class DbUnifier {

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
				this.closeConnection(con);
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
				DatabaseMetaData databaseMetaData = con.getMetaData();
				DbFeature dbFeature = DbFeature.getInstance(databaseMetaData);
				String schema = dbFeature.getDatabaseSchema(databaseMetaData);
				String[] types = { "TABLE" };
				tableResultSet = databaseMetaData.getTables(null, schema, "%",
						types);
				while (tableResultSet.next()) {
					String tableName = tableResultSet.getString("TABLE_NAME");
					tableName = tableName.toUpperCase();
					if (tableName.startsWith("BIN$")) {
						continue;
					}
					Set<String> primaryKeySet = new HashSet<String>();
					ResultSet primaryKeyResultSet = null;
					try {
						primaryKeyResultSet = databaseMetaData.getPrimaryKeys(
								null, schema, tableName);
						while (primaryKeyResultSet.next()) {
							String columnName = primaryKeyResultSet
									.getString("COLUMN_NAME");
							columnName = columnName.toUpperCase();
							primaryKeySet.add(columnName);
						}
					} finally {
						this.closeResultSet(primaryKeyResultSet);
					}
					List<Column> columnList = new ArrayList<Column>();
					ResultSet columnResultSet = null;
					try {
						columnResultSet = databaseMetaData.getColumns(null,
								schema, tableName, "%");
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
							column.setType(this.getColumnType(dataType));
							column.setSize(size);
							column.setNullable(nullable != ResultSetMetaData.columnNoNulls);
							column.setIsPrimaryKey(primaryKeySet
									.contains(columnName));
							columnList.add(column);
						}
					} finally {
						this.closeResultSet(columnResultSet);
					}
					Table table = new Table();
					table.setName(tableName);
					table.addColumnList(columnList);
					tableList.add(table);
				}
			} finally {
				this.closeResultSet(tableResultSet);
			}
			return tableList;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				this.closeConnection(con);
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
			DatabaseMetaData databaseMetaData = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(databaseMetaData);
			String schema = dbFeature.getDatabaseSchema(databaseMetaData);
			tableName = tableName.toUpperCase();
			Set<String> primaryKeySet = new HashSet<String>();
			ResultSet primaryKeyResultSet = null;
			try {
				primaryKeyResultSet = databaseMetaData.getPrimaryKeys(null,
						schema, tableName);
				while (primaryKeyResultSet.next()) {
					String columnName = primaryKeyResultSet
							.getString("COLUMN_NAME");
					columnName = columnName.toUpperCase();
					primaryKeySet.add(columnName);
				}
			} finally {
				this.closeResultSet(primaryKeyResultSet);
			}
			List<Column> columnList = new ArrayList<Column>();
			ResultSet columnResultSet = null;
			try {
				columnResultSet = databaseMetaData.getColumns(null, schema,
						tableName, "%");
				while (columnResultSet.next()) {
					String columnName = columnResultSet
							.getString("COLUMN_NAME");
					columnName = columnName.toUpperCase();
					short dataType = columnResultSet.getShort("DATA_TYPE");
					int size = columnResultSet.getInt("COLUMN_SIZE");
					int nullable = columnResultSet.getInt("NULLABLE");
					Column column = new Column();
					column.setName(columnName);
					column.setType(this.getColumnType(dataType));
					column.setSize(size);
					column.setNullable(nullable != ResultSetMetaData.columnNoNulls);
					column.setIsPrimaryKey(primaryKeySet.contains(columnName));
					columnList.add(column);
				}
			} finally {
				this.closeResultSet(columnResultSet);
			}
			Table table = new Table();
			table.setName(tableName);
			table.addColumnList(columnList);
			return table;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				this.closeConnection(con);
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
				DatabaseMetaData databaseMetaData = con.getMetaData();
				DbFeature dbFeature = DbFeature.getInstance(databaseMetaData);
				String schema = dbFeature.getDatabaseSchema(databaseMetaData);
				String[] types = { "VIEW" };
				viewResultSet = databaseMetaData.getTables(null, schema, "%",
						types);
				while (viewResultSet.next()) {
					String viewName = viewResultSet.getString("TABLE_NAME");
					viewName = viewName.toUpperCase();
					List<Column> columnList = new ArrayList<Column>();
					ResultSet columnResultSet = null;
					try {
						columnResultSet = databaseMetaData.getColumns(null,
								schema, viewName, "%");
						while (columnResultSet.next()) {
							String columnName = columnResultSet
									.getString("COLUMN_NAME");
							columnName = columnName.toUpperCase();
							short dataType = columnResultSet
									.getShort("DATA_TYPE");
							int size = columnResultSet.getInt("COLUMN_SIZE");
							Column column = new Column();
							column.setName(columnName);
							column.setType(this.getColumnType(dataType));
							column.setSize(size);
							column.setNullable(true);
							column.setIsPrimaryKey(false);
							columnList.add(column);
						}
					} finally {
						this.closeResultSet(columnResultSet);
					}
					Table view = new Table();
					view.setName(viewName);
					view.addColumnList(columnList);
					viewList.add(view);
				}
			} finally {
				this.closeResultSet(viewResultSet);
			}
			return viewList;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				this.closeConnection(con);
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
			DatabaseMetaData databaseMetaData = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(databaseMetaData);
			String schema = dbFeature.getDatabaseSchema(databaseMetaData);
			viewName = viewName.toUpperCase();
			List<Column> columnList = new ArrayList<Column>();
			ResultSet columnResultSet = null;
			try {
				columnResultSet = databaseMetaData.getColumns(null, schema,
						viewName, "%");
				while (columnResultSet.next()) {
					String columnName = columnResultSet
							.getString("COLUMN_NAME");
					columnName = columnName.toUpperCase();
					short dataType = columnResultSet.getShort("DATA_TYPE");
					int size = columnResultSet.getInt("COLUMN_SIZE");
					Column column = new Column();
					column.setName(columnName);
					column.setType(this.getColumnType(dataType));
					column.setSize(size);
					column.setNullable(true);
					column.setIsPrimaryKey(false);
					columnList.add(column);
				}
			} finally {
				this.closeResultSet(columnResultSet);
			}
			Table view = new Table();
			view.setName(viewName);
			view.addColumnList(columnList);
			return view;
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				this.closeConnection(con);
			}
		}
	}

	public RowSet executeSelectSql(String sql, Values values,
			boolean containsLob, int pageSize, int pageNumber) {
		Connection con;
		if (this.con == null) {
			con = dbConfig.getConnection();
		} else {
			con = this.con;
		}
		try {
			DatabaseMetaData databaseMetaData = con.getMetaData();
			DbFeature dbFeature = DbFeature.getInstance(databaseMetaData);
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
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
					if (count % 2 == 1) {
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
				PreparedStatement ps2 = null;
				ResultSet rs2 = null;
				try {
					ps2 = con.prepareStatement(countSql);
					this.setColumnValue(ps2, 1, values);
					rs2 = ps2.executeQuery();
					if (rs2.next()) {
						totalRowCount = rs2.getBigDecimal(1).intValue();
					} else {
						throw new DbUnifierException("no resultset");
					}
				} catch (SQLException e) {
					throw new DbUnifierException(e);
				} finally {
					this.closeResultSet(rs2);
					this.closeStatement(ps2);
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
					ps = con.prepareStatement(sql,
							ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_READ_ONLY);
				} else {
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
						throw new DbUnifierException(
								"abnormal resultset location");
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
						String type = this.getColumnType(dataType);
						Object value = null;
						if (Column.TYPE_STRING.equals(type)) {
							value = rs.getString(i + 1);
						} else if (Column.TYPE_NUMBER.equals(type)) {
							value = rs.getBigDecimal(i + 1);
						} else if (Column.TYPE_DATETIME.equals(type)) {
							value = rs.getTimestamp(i + 1);
						} else if (Column.TYPE_CLOB.equals(type)) {
							if (containsLob) {
								Reader reader = rs.getCharacterStream(i + 1);
								if (reader != null) {
									StringWriter sw = new StringWriter();
									char[] buffer = new char[2048];
									int charsRead;
									try {
										while ((charsRead = reader.read(buffer,
												0, 1024)) != -1) {
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
										while ((bytesRead = is.read(buffer, 0,
												1024)) != -1) {
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
								}
							}
						} else {
							throw new DbUnifierException(
									"not support data type");
						}
						row.addValue(columnName, value);
					}
					rowSet.addRow(row);
				}
				return rowSet;
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			} finally {
				this.closeResultSet(rs);
				this.closeStatement(ps);
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} finally {
			if (this.con == null) {
				this.closeConnection(con);
			}
		}
	}

	public RowSet executeSelectSql(String sql, Values values, int pageSize,
			int pageNumber) {
		return this.executeSelectSql(sql, values, false, pageSize, pageNumber);
	}

	public RowSet executeSelectSql(String sql, int pageSize, int pageNumber) {
		Values values = new Values();
		return this.executeSelectSql(sql, values, false, pageSize, pageNumber);
	}

	public RowSet executeSelectSql(String sql, Values values,
			boolean containsLob) {
		return null;
	}

	public RowSet executeSelectSql(String sql, Values values) {
		return this.executeSelectSql(sql, values, false);
	}

	public RowSet executeSelectSql(String sql) {
		Values values = new Values();
		return this.executeSelectSql(sql, values, false);
	}

	public int executeOtherSql(String sql, Values values) {
		return 0;
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
			return Column.TYPE_DATETIME;
		} else if (dataType == Types.DATE) {
			return Column.TYPE_DATETIME;
		} else if (dataType == Types.TIME) {
			return Column.TYPE_DATETIME;
		} else if (dataType == Types.BLOB) {
			return Column.TYPE_BLOB;
		} else if (dataType == Types.BINARY) {
			return Column.TYPE_BLOB;
		} else if (dataType == Types.LONGVARBINARY) {
			return Column.TYPE_BLOB;
		} else if (dataType == Types.CLOB) {
			return Column.TYPE_CLOB;
		} else if (dataType == Types.OTHER) {
			return Column.TYPE_STRING;
		} else {
			throw new DbUnifierException("unknown data type");
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
				} else if (Column.TYPE_DATETIME.equals(type)) {
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
				char[] ch = (char[]) value;
				Reader reader = new StringReader(new String(ch));
				ps.setCharacterStream(pos, reader, ch.length);
			} else if (value instanceof byte[]) {
				byte[] by = (byte[]) value;
				InputStream is = new ByteArrayInputStream(by);
				ps.setBinaryStream(pos, is, by.length);
			}
		}
	}

}
