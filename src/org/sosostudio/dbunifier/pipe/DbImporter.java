package org.sosostudio.dbunifier.pipe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.sosostudio.dbunifier.Column;
import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.DbUnifierException;
import org.sosostudio.dbunifier.DbUtil;
import org.sosostudio.dbunifier.DbXmlConfig;
import org.sosostudio.dbunifier.Table;

public class DbImporter {

	private static Map<String, PipeSql> pipeSqlMap = new HashMap<String, PipeSql>();

	public static void main(String[] args) {
		Connection con = null;
		FileInputStream xmlis = null;
		XMLEventReader xmlr = null;
		try {
			con = DbXmlConfig.dbConfig.getConnection();
			DbUnifier unifier = new DbUnifier(con);
			// produce name mapping
			Map<String, String> map = new HashMap<String, String>();
			Map<String, Table> tableMap = new HashMap<String, Table>();
			List<Table> tableList = unifier.getTableList();
			for (Table table : tableList) {
				tableMap.put(table.getName(), table);
			}

			XMLInputFactory xmlif = XMLInputFactory.newInstance();
			xmlis = new FileInputStream("result/data.xml");
			xmlr = xmlif.createXMLEventReader(xmlis, "UTF-8");

			while (xmlr.hasNext()) {
				try {
					XMLEvent event = xmlr.nextEvent();
					if (event.getEventType() == XMLStreamReader.START_ELEMENT) {
						StartElement element = event.asStartElement();
						String elementName = element.getName().getLocalPart();
						if ("m".equals(elementName)) {
							String n = element.getAttributeByName(
									new QName("n")).getValue();
							String s = element.getAttributeByName(
									new QName("s")).getValue();
							map.put(s, n);
						} else if ("t".equals(elementName)) {
							String tableSeq = element.getAttributeByName(
									new QName("s")).getValue();
							String tableName = map.get(tableSeq);
							Table table = tableMap.get(tableName);
							Map<String, String> valueMap = new HashMap<String, String>();
							while (xmlr.hasNext()) {
								XMLEvent event2 = xmlr.nextEvent();
								if (event2.getEventType() == XMLStreamReader.START_ELEMENT) {
									StartElement element2 = event2
											.asStartElement();
									String columnSeq = element2
											.getAttributeByName(new QName("s"))
											.getValue();
									String columnName2 = map.get(columnSeq);
									Attribute vAttribute = element2
											.getAttributeByName(new QName("v"));
									String columnValue2;
									if (vAttribute == null) {
										columnValue2 = null;
									} else {
										columnValue2 = vAttribute.getValue();
									}
									valueMap.put(columnName2, columnValue2);
								}
							}
							PipeSql pipeSql = getPipeSql(table, valueMap);
							{
								String insertSql = pipeSql.getInsertSql();
								List<NameType> nameTypeList = pipeSql
										.getInsertNameTypeList();
								PreparedStatement ps = null;
								try {
									ps = con.prepareStatement(insertSql);
									for (int i = 0; i < nameTypeList.size(); i++) {
										NameType nameType = nameTypeList.get(i);
										String columnName = nameType.getName();
										String sValue = valueMap
												.get(columnName);
										ColumnType type = nameType.getType();
										if (type == ColumnType.TYPE_STRING) {
											ps.setString(i + 1, sValue);
										} else if (type == ColumnType.TYPE_NUMBER) {
											if (sValue == null) {
												ps.setBigDecimal(i + 1, null);
											} else {
												ps.setBigDecimal(i + 1,
														new BigDecimal(sValue));
											}
										} else if (type == ColumnType.TYPE_TIMESTAMP) {
											if (sValue == null) {
												ps.setTimestamp(i + 1, null);
											} else {
												ps.setTimestamp(
														i + 1,
														new Timestamp(
																Long.parseLong(sValue)));
											}
										} else if (type == ColumnType.TYPE_CLOB) {
											if (sValue == null) {
												ps.setCharacterStream(i + 1,
														null, 0);
											} else {
												FileInputStream is = null;
												InputStreamReader reader = null;
												try {
													File file = new File(sValue);
													is = new FileInputStream(
															file);
													reader = new InputStreamReader(
															is);
													ps.setCharacterStream(
															i + 1, reader,
															(int) file.length());
												} finally {
													try {
														if (reader != null) {
															reader.close();
														}
														if (is != null) {
															is.close();
														}
													} catch (IOException e) {
														throw new DbUnifierException(
																e);
													}
												}
											}
										} else if (type == ColumnType.TYPE_BLOB) {
											if (sValue == null) {
												ps.setBinaryStream(i + 1, null,
														0);
											} else {
												FileInputStream is = null;
												try {
													File file = new File(sValue);
													is = new FileInputStream(
															file);
													ps.setBinaryStream(i + 1,
															is,
															(int) file.length());
												} finally {
													try {
														if (is != null) {
															is.close();
														}
													} catch (IOException e) {
														throw new DbUnifierException(
																e);
													}
												}
											}
										}
									}
									ps.executeUpdate();
								} catch (SQLException e) {
									e.printStackTrace();
								} finally {
									DbUtil.closeStatement(ps);
								}
							}
						}
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (XMLStreamException e) {
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e) {
			throw new DbUnifierException(e);
		} catch (XMLStreamException e) {
			throw new DbUnifierException(e);
		} finally {
			try {
				if (xmlr != null) {
					xmlr.close();
				}
			} catch (XMLStreamException e) {
				throw new DbUnifierException(e);
			}
			try {
				if (xmlis != null) {
					xmlis.close();
				}
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
			DbUtil.closeConnection(con);
		}
	}

	private static PipeSql getPipeSql(Table table, Map<String, String> valueMap) {
		String tableName = table.getName();
		PipeSql pipeSql = pipeSqlMap.get(tableName);
		if (pipeSql == null) {
			pipeSql = new PipeSql();
			{
				StringBuilder insertSqlSb = new StringBuilder();
				insertSqlSb.append("insert into ").append(tableName)
						.append("(");
				int count = 0;
				for (Column column : table.getColumnList()) {
					String columnName = column.getName();
					if (valueMap.containsKey(columnName)) {
						if (count > 0) {
							insertSqlSb.append(", ");
						}
						insertSqlSb.append(columnName);
						pipeSql.addInsertNameType(new NameType(columnName,
								column.getType()));
						count++;
					}
				}
				insertSqlSb.append(") values(");
				for (int i = 0; i < count; i++) {
					if (i > 0) {
						insertSqlSb.append(", ");
					}
					insertSqlSb.append("?");
				}
				insertSqlSb.append(")");
				pipeSql.setInsertSql(insertSqlSb.toString());
			}
			{
				StringBuilder updateSqlSb = new StringBuilder();
				updateSqlSb.append("update ").append(tableName).append(" set ");
				int count = 0;
				for (Column column : table.getColumnList()) {
					if (column.getIsPrimaryKey()) {
						continue;
					}
					String columnName = column.getName();
					if (valueMap.containsKey(columnName)) {
						if (count > 0) {
							updateSqlSb.append(", ");
						}
						updateSqlSb.append(columnName).append(" = ?");
						pipeSql.addUpdateNameType(new NameType(columnName,
								column.getType()));
						count++;
					}
				}
				boolean needUpdate = count != 0;
				pipeSql.setNeedUpdate(needUpdate);
				if (!needUpdate) {
					return pipeSql;
				}
				updateSqlSb.append(" where ");
				count = 0;
				for (Column column : table.getColumnList()) {
					if (!column.getIsPrimaryKey()) {
						continue;
					}
					String columnName = column.getName();
					if (count > 0) {
						updateSqlSb.append(" and ");
					}
					updateSqlSb.append(columnName).append(" = ?");
					pipeSql.addUpdateNameType(new NameType(columnName, column
							.getType()));
					count++;
				}
				updateSqlSb.append(")");
				pipeSql.setUpdateSql(updateSqlSb.toString());
			}
		}
		return pipeSql;
	}
}