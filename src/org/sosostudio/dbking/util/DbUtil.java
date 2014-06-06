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

package org.sosostudio.dbking.util;

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

import org.sosostudio.dbking.ColumnType;
import org.sosostudio.dbking.NullValue;
import org.sosostudio.dbking.Values;
import org.sosostudio.dbking.config.XmlConfig;

public class DbUtil {

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
			return ColumnType.STRING;
		} else if (dataType == Types.NVARCHAR) {
			return ColumnType.STRING;
		} else if (dataType == Types.LONGVARCHAR) {
			return ColumnType.STRING;
		} else if (dataType == Types.LONGNVARCHAR) {
			return ColumnType.STRING;
		} else if (dataType == Types.CHAR) {
			return ColumnType.STRING;
		} else if (dataType == Types.NCHAR) {
			return ColumnType.STRING;
		} else if (dataType == Types.INTEGER) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.BIGINT) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.SMALLINT) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.TINYINT) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.FLOAT) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.DOUBLE) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.DECIMAL) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.REAL) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.NUMERIC) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.BIT) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.BOOLEAN) {
			return ColumnType.NUMBER;
		} else if (dataType == Types.TIMESTAMP) {
			return ColumnType.TIMESTAMP;
		} else if (dataType == Types.DATE) {
			return ColumnType.TIMESTAMP;
		} else if (dataType == Types.TIME) {
			return ColumnType.TIMESTAMP;
		} else if (dataType == Types.BLOB) {
			return ColumnType.BLOB;
		} else if (dataType == Types.BINARY) {
			return ColumnType.BLOB;
		} else if (dataType == Types.VARBINARY) {
			return ColumnType.BLOB;
		} else if (dataType == Types.LONGVARBINARY) {
			return ColumnType.BLOB;
		} else if (dataType == Types.CLOB) {
			return ColumnType.CLOB;
		} else if (dataType == Types.NCLOB) {
			return ColumnType.CLOB;
		} else {
			return ColumnType.STRING;
		}
	}

	public static boolean isNationalString(String dbType) {
		dbType = dbType.toUpperCase();
		System.out.println(dbType);
		return dbType.startsWith("NATIONAL") || dbType.startsWith("NCHAR")
				|| dbType.startsWith("NVARCHAR");
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
				if (type == ColumnType.STRING) {
					ps.setString(pos, null);
				} else if (type == ColumnType.NUMBER) {
					ps.setBigDecimal(pos, null);
				} else if (type == ColumnType.TIMESTAMP) {
					ps.setTimestamp(pos, null);
				} else if (type == ColumnType.CLOB) {
					ps.setCharacterStream(pos, null, 0);
				} else if (type == ColumnType.BLOB) {
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
		if (XmlConfig.needsShowSql()) {
			StringBuilder sb = new StringBuilder();
			sb.append("\n").append(sql).append("\n");
			if (values != null) {
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
			}
			System.out.println(sb.toString());
		}
	}

}
