package org.sosostudio.dbunifier.pipe;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.sosostudio.dbunifier.Column;
import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.config.XmlConfig;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.DbUtil;

public class DbExporter {

	public static void main(String[] args) {
		Connection con = null;
		OutputStream xmlos = null;
		XMLStreamWriter xmlw = null;
		try {
			con = XmlConfig.getDbSource("export").getConnection();
			DbUnifier unifier = new DbUnifier(con);
			// produce name mapping and table mapping
			List<Table> tableList = unifier.getTableList();
			Map<String, Table> tableMap = new HashMap<String, Table>();
			Map<String, String> map = new HashMap<String, String>();
			int num = 1;
			for (Table table : tableList) {
				String tableName = table.getName();
				tableMap.put(tableName, table);
				if (!map.containsKey(tableName)) {
					map.put(tableName, num + "");
					num++;
				}
				for (Column column : table.getColumnList()) {
					String columnName = column.getName();
					if (!map.containsKey(columnName)) {
						map.put(columnName, num + "");
						num++;
					}
				}
			}
			// initialize parameters
			if (args.length == 0) {
				args = new String[tableList.size()];
				for (int m = 0; m < tableList.size(); m++) {
					Table table = tableList.get(m);
					args[m] = table.getName();
				}
			}
			// create xml
			new File("result").mkdirs();
			XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
			xmlos = new BufferedOutputStream(new FileOutputStream(
					"result/data.xml"));
			xmlw = xmlof.createXMLStreamWriter(xmlos, "UTF-8");
			xmlw.writeStartDocument();
			xmlw.writeStartElement("root");
			for (String key : map.keySet()) {
				String value = map.get(key);
				xmlw.writeStartElement("m");
				xmlw.writeAttribute("n", key);
				xmlw.writeAttribute("s", value);
				xmlw.writeEndElement();
			}
			// loop tables
			for (int m = 0; m < args.length; m++) {
				String[] params = args[m].split("\\|");
				// extract parameters
				String tableAndAlias = params[0];
				int pos = tableAndAlias.indexOf(" ");
				String tableName = pos >= 0 ? tableAndAlias.substring(0, pos)
						: tableAndAlias;
				tableName = tableName.toUpperCase();
				String fields = "*";
				if (params.length > 1) {
					fields = params[1];
				}
				String conditions = null;
				if (params.length > 2) {
					conditions = (String) params[2];
				}
				System.out.println(" ----------------------------------- ");
				System.out.println(" ----------------------------------- ");
				System.out.println(" ----------------------------------- ");
				System.out.println(" start exporting " + tableName);
				System.out.println(" ----------------------------------- ");
				System.out.println(" ----------------------------------- ");
				System.out.println(" ----------------------------------- ");
				System.out.println();
				// get table info
				Table table = tableMap.get(tableName);
				if (table == null) {
					System.out.println(tableName + " not exists");
					continue;
				}
				// format sql
				String sql = "select " + fields + " from " + tableAndAlias;
				if (conditions != null) {
					sql += " where " + conditions;
				}
				// export
				Statement statement = null;
				ResultSet rs = null;
				try {
					// query
					statement = con.createStatement();
					rs = statement.executeQuery(sql);
					ResultSetMetaData rsmd = rs.getMetaData();
					int columnCount = rsmd.getColumnCount();
					while (rs.next()) {
						System.out.println(tableName + " - " + rs.getRow());
						xmlw.writeStartElement("t");
						xmlw.writeAttribute("s", map.get(tableName));
						for (int i = 1; i <= columnCount; i++) {
							xmlw.writeStartElement("c");
							String columnName = rsmd.getColumnLabel(i);
							columnName = columnName.toUpperCase();
							xmlw.writeAttribute("s", map.get(columnName));
							int dataType = rsmd.getColumnType(i);
							ColumnType type = DbUtil.getColumnType(dataType);
							if (type == ColumnType.TYPE_STRING) {
								String value = rs.getString(i);
								if (value != null) {
									xmlw.writeAttribute("v", value);
								}
							} else if (type == ColumnType.TYPE_NUMBER) {
								BigDecimal value = rs.getBigDecimal(i);
								if (value != null) {
									xmlw.writeAttribute("v", value.toString());
								}
							} else if (type == ColumnType.TYPE_TIMESTAMP) {
								Timestamp value = rs.getTimestamp(i);
								if (value != null) {
									xmlw.writeAttribute("v", value.getTime()
											+ "");
								}
							} else if (type == ColumnType.TYPE_CLOB) {
								Reader reader = rs.getCharacterStream(i);
								if (reader != null) {
									String filename = "result/"
											+ UUID.randomUUID() + ".txt";
									Writer writer = null;
									char[] buffer = new char[2048];
									int charsRead;
									try {
										writer = new BufferedWriter(
												new FileWriter(filename));
										while ((charsRead = reader.read(buffer,
												0, 1024)) != -1) {
											writer.write(buffer, 0, charsRead);
										}
									} catch (IOException e) {
										throw new DbUnifierException(e);
									} finally {
										try {
											if (writer != null) {
												writer.close();
											}
											reader.close();
										} catch (IOException e) {
											throw new DbUnifierException(e);
										}
									}
									xmlw.writeAttribute("v", filename);
								}
							} else if (type == ColumnType.TYPE_BLOB) {
								InputStream is = rs.getBinaryStream(i);
								if (is != null) {
									String filename = "result/"
											+ UUID.randomUUID() + ".dat";
									OutputStream os = null;
									byte[] buffer = new byte[2048];
									int bytesRead;
									try {
										os = new BufferedOutputStream(
												new FileOutputStream(filename));
										while ((bytesRead = is.read(buffer, 0,
												1024)) != -1) {
											os.write(buffer, 0, bytesRead);
										}
									} catch (IOException e) {
										throw new DbUnifierException(e);
									} finally {
										try {
											if (os != null) {
												os.close();
											}
											is.close();
										} catch (IOException e) {
											throw new DbUnifierException(e);
										}
									}
									xmlw.writeAttribute("v", filename);
								}
							} else {
								System.out.println("not support data type: "
										+ columnName);
								continue;
							}
							xmlw.writeEndElement();
						}
						xmlw.writeEndElement();
					}
				} catch (SQLException e) {
					throw new DbUnifierException(e);
				} finally {
					DbUtil.closeResultSet(rs);
					DbUtil.closeStatement(statement);
				}
			}
			xmlw.writeEndElement();
			xmlw.writeEndDocument();
			xmlw.flush();
		} catch (FileNotFoundException e) {
			throw new DbUnifierException(e);
		} catch (XMLStreamException e) {
			throw new DbUnifierException(e);
		} finally {
			try {
				if (xmlw != null) {
					xmlw.close();
				}
			} catch (XMLStreamException e) {
				throw new DbUnifierException(e);
			}
			try {
				if (xmlos != null) {
					xmlos.close();
				}
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
			DbUtil.closeConnection(con);
		}
	}

}
