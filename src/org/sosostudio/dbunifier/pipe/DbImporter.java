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
 */

package org.sosostudio.dbunifier.pipe;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
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
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.sosostudio.dbunifier.Column;
import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.Table;
import org.sosostudio.dbunifier.config.XmlConfig;
import org.sosostudio.dbunifier.dbsource.DbSource;
import org.sosostudio.dbunifier.util.DbUnifierException;
import org.sosostudio.dbunifier.util.DbUtil;
import org.sosostudio.dbunifier.util.IoUtil;

public class DbImporter {

	private static final String CONFIG_NAME = "importer";

	private static final String PARAM_IGNORE_ERROR = "ignore_error";

	private static Map<String, PipeSql> pipeSqlMap = new HashMap<String, PipeSql>();

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		boolean ignoreError = false;
		if (args.length > 0) {
			ignoreError = PARAM_IGNORE_ERROR.equalsIgnoreCase(args[1]);
		}
		Connection con = null;
		InputStream xmlis = null;
		XMLEventReader xmlr = null;
		Reader clobr = null;
		InputStream blobis = null;
		try {
			DbSource dbSource = XmlConfig.getDbSource(CONFIG_NAME);
			System.out.println("You will operate the following database:");
			System.out.println(dbSource);
			System.out
					.println("Please confirm the database you really want to operate, (Y)es for going on or (N)o for breaking?");
			while (true) {
				int ch = System.in.read();
				if (ch == 'Y' || ch == 'y') {
					break;
				} else if (ch == 'N' || ch == 'n') {
					return;
				}
			}
			con = dbSource.getConnection();
			DbUnifier unifier = new DbUnifier(con);
			// produce name mapping
			Map<String, String> map = new HashMap<String, String>();
			Map<String, Table> tableMap = new HashMap<String, Table>();
			List<Table> tableList = unifier.getTableList();
			for (Table table : tableList) {
				tableMap.put(table.getName(), table);
			}
			// extract xml
			XMLInputFactory xmlif = XMLInputFactory.newInstance();
			xmlis = new BufferedInputStream(new FileInputStream(
					"result/data.xml"));
			xmlr = xmlif.createXMLEventReader(xmlis, "UTF-8");
			// extract blob and clob content from bin and txt files
			clobr = new BufferedReader(new FileReader("result/data.txt"));
			int clobStartPos = 0;
			blobis = new BufferedInputStream(new FileInputStream(
					"result/data.bin"));
			int blobStartPos = 0;
			// loop xml
			int rowCount = 0;
			while (xmlr.hasNext()) {
				XMLEvent event = xmlr.nextEvent();
				if (event.getEventType() == XMLStreamReader.START_ELEMENT) {
					StartElement element = event.asStartElement();
					String elementName = element.getName().getLocalPart();
					if ("m".equals(elementName)) {
						String n = element.getAttributeByName(new QName("n"))
								.getValue();
						String s = element.getAttributeByName(new QName("s"))
								.getValue();
						map.put(s, n);
					} else if ("i".equals(elementName)) {
						String tableName = element.getAttributeByName(
								new QName("n")).getValue();
						Table table = tableMap.get(tableName);
						if (table == null) {
							String filename = element.getAttributeByName(
									new QName("f")).getValue();
							FileInputStream fis = null;
							try {
								fis = new FileInputStream(filename);
								ObjectInputStream ois = new ObjectInputStream(
										fis);
								table = (Table) ois.readObject();
								unifier.createTable(table);
								tableMap.put(tableName, table);
							} finally {
								IoUtil.closeInputStream(fis);
							}
						}
					} else if ("t".equals(elementName)) {
						rowCount++;
						String tableSeq = element.getAttributeByName(
								new QName("s")).getValue();
						String tableName = map.get(tableSeq);
						System.out.println(rowCount + " - " + tableName);
						Table table = tableMap.get(tableName);
						Map<String, String> valueMap = new HashMap<String, String>();
						while (xmlr.hasNext()) {
							XMLEvent event2 = xmlr.nextEvent();
							int eventType2 = event2.getEventType();
							if (eventType2 == XMLStreamReader.START_ELEMENT) {
								StartElement element2 = event2.asStartElement();
								String columnSeq = element2.getAttributeByName(
										new QName("s")).getValue();
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
							} else if (eventType2 == XMLStreamReader.END_ELEMENT) {
								EndElement element2 = event2.asEndElement();
								if ("t".equals(element2.getName()
										.getLocalPart())) {
									break;
								}
							}
						}
						PipeSql pipeSql = getPipeSql(table, valueMap);
						{
							String sql = pipeSql.getSql();
							List<NameType> nameTypeList = pipeSql
									.getNameTypeList();
							PreparedStatement ps = null;
							try {
								ps = con.prepareStatement(sql);
								for (int i = 1; i <= nameTypeList.size(); i++) {
									NameType nameType = nameTypeList.get(i - 1);
									String columnName = nameType.getName();
									String sValue = valueMap.get(columnName);
									ColumnType type = nameType.getType();
									if (type == ColumnType.TYPE_STRING) {
										ps.setString(i, sValue);
									} else if (type == ColumnType.TYPE_NUMBER) {
										if (sValue == null) {
											ps.setBigDecimal(i, null);
										} else {
											ps.setBigDecimal(i, new BigDecimal(
													sValue));
										}
									} else if (type == ColumnType.TYPE_TIMESTAMP) {
										if (sValue == null) {
											ps.setTimestamp(i, null);
										} else {
											ps.setTimestamp(i, new Timestamp(
													Long.parseLong(sValue)));
										}
									} else if (type == ColumnType.TYPE_CLOB) {
										if (sValue == null) {
											ps.setCharacterStream(i, null, 0);
										} else {
											String[] s = sValue.split(",");
											int pos = Integer.parseInt(s[0]);
											int length = Integer.parseInt(s[1]);
											if (clobStartPos != pos) {
												clobr.skip(pos - clobStartPos);
												clobStartPos = pos;
											}
											ps.setCharacterStream(i, clobr,
													length);
											clobStartPos += length;
										}
									} else if (type == ColumnType.TYPE_BLOB) {
										if (sValue == null) {
											ps.setBinaryStream(i, null, 0);
										} else {
											String[] s = sValue.split(",");
											int pos = Integer.parseInt(s[0]);
											int length = Integer.parseInt(s[1]);
											if (blobStartPos != pos) {
												blobis.skip(pos - blobStartPos);
												blobStartPos = pos;
											}
											ps.setBinaryStream(i, blobis,
													length);
											blobStartPos += length;
										}
									}
								}
								ps.executeUpdate();
							} catch (SQLException e) {
								if (ignoreError) {
									e.printStackTrace();
								} else {
									throw e;
								}
							} catch (IOException e) {
								if (ignoreError) {
									e.printStackTrace();
								} else {
									throw e;
								}
							} finally {
								DbUtil.closeStatement(ps);
							}
						}
					}
				}
			}
		} catch (ClassNotFoundException e) {
			throw new DbUnifierException(e);
		} catch (SQLException e) {
			throw new DbUnifierException(e);
		} catch (IOException e) {
			throw new DbUnifierException(e);
		} catch (XMLStreamException e) {
			throw new DbUnifierException(e);
		} finally {
			IoUtil.closeInputStream(blobis);
			IoUtil.closeReader(clobr);
			IoUtil.closeReader(xmlr);
			IoUtil.closeInputStream(xmlis);
			DbUtil.closeConnection(con);
		}
		long end = System.currentTimeMillis();
		System.out.println("It took up " + (end - start) / 1000 + " minutes.");
	}

	private static PipeSql getPipeSql(Table table, Map<String, String> valueMap) {
		String tableName = table.getName();
		PipeSql pipeSql = pipeSqlMap.get(tableName);
		if (pipeSql == null) {
			pipeSql = new PipeSql();
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(tableName).append("(");
			int count = 0;
			for (Column column : table.getColumnList()) {
				String columnName = column.getName();
				if (valueMap.containsKey(columnName)) {
					if (count > 0) {
						sb.append(", ");
					}
					sb.append(columnName);
					pipeSql.addNameType(new NameType(columnName, column
							.getType()));
					count++;
				}
			}
			sb.append(") values(");
			for (int i = 0; i < count; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append("?");
			}
			sb.append(")");
			pipeSql.setSql(sb.toString());
		}
		return pipeSql;
	}
}