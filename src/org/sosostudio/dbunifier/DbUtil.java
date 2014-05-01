package org.sosostudio.dbunifier;

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

public class DbUtil {

	public static void closeConnection(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		}
	}

	public static void closeStatement(Statement statement) {
		try {
			if (statement != null) {
				statement.close();
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		}
	}

	public static void closeResultSet(ResultSet resultSet) {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		}
	}

	public static String getColumnType(int dataType) {
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
			System.out.println(dataType);
			return Column.TYPE_UNKNOWN;
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
		System.out.println(sb.toString());
	}

}
