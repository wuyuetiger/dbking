package org.sosostudio.dbunifier.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import javax.sql.DataSource;

import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.NullValue;
import org.sosostudio.dbunifier.Values;
import org.sosostudio.dbunifier.config.XmlConfig;

public class DbUtil {

	public static Connection getConnection(DataSource dataSource) {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		}
	}

	public static void closeConnection(Connection con) {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeStatement(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeResultSet(ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static ColumnType getColumnType(int dataType) {
		if (dataType == Types.VARCHAR) {
			return ColumnType.TYPE_STRING;
		} else if (dataType == Types.LONGVARCHAR) {
			return ColumnType.TYPE_STRING;
		} else if (dataType == Types.NVARCHAR) {
			return ColumnType.TYPE_STRING;
		} else if (dataType == Types.CHAR) {
			return ColumnType.TYPE_STRING;
		} else if (dataType == Types.INTEGER) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.BIGINT) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.SMALLINT) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.TINYINT) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.FLOAT) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.DOUBLE) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.DECIMAL) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.REAL) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.NUMERIC) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.BIT) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.BOOLEAN) {
			return ColumnType.TYPE_NUMBER;
		} else if (dataType == Types.TIMESTAMP) {
			return ColumnType.TYPE_TIMESTAMP;
		} else if (dataType == Types.DATE) {
			return ColumnType.TYPE_TIMESTAMP;
		} else if (dataType == Types.TIME) {
			return ColumnType.TYPE_TIMESTAMP;
		} else if (dataType == Types.BLOB) {
			return ColumnType.TYPE_BLOB;
		} else if (dataType == Types.BINARY) {
			return ColumnType.TYPE_BLOB;
		} else if (dataType == Types.VARBINARY) {
			return ColumnType.TYPE_BLOB;
		} else if (dataType == Types.LONGVARBINARY) {
			return ColumnType.TYPE_BLOB;
		} else if (dataType == Types.CLOB) {
			return ColumnType.TYPE_CLOB;
		} else {
			return ColumnType.TYPE_UNKNOWN;
		}
	}

	public static void setColumnValue(PreparedStatement ps, int startPos,
			Values values) throws SQLException {
		List<Object> valueList = values.getValueList();
		for (int i = 0; i < valueList.size(); i++) {
			int pos = startPos + i;
			Object value = valueList.get(i);
			if (value instanceof NullValue) {
				NullValue nullValue = (NullValue) value;
				ColumnType type = nullValue.getType();
				if (type == ColumnType.TYPE_STRING) {
					ps.setString(pos, null);
				} else if (type == ColumnType.TYPE_NUMBER) {
					ps.setBigDecimal(pos, null);
				} else if (type == ColumnType.TYPE_TIMESTAMP) {
					ps.setTimestamp(pos, null);
				} else if (type == ColumnType.TYPE_CLOB) {
					ps.setCharacterStream(pos, null, 0);
				} else if (type == ColumnType.TYPE_BLOB) {
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

	public static void printSql(String sql, Values values) {
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
		if (XmlConfig.needsShowSql()) {
			System.out.println(sb.toString());
		}
	}

}
