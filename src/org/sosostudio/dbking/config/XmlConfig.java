/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2014 YU YUE, SOSO STUDIO, wuyuetiger@gmail.com
 *
 * License: GNU Lesser General Public License (LGPL)
 * 
 * Source code availability:
 *  https://github.com/wuyuetiger/dbking
 *  https://code.csdn.net/tigeryu/dbking
 *  https://git.oschina.net/db-unifier/dbking
 */

package org.sosostudio.dbking.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.sosostudio.dbking.dbsource.DbSource;
import org.sosostudio.dbking.dbsource.JdbcDbSource;
import org.sosostudio.dbking.dbsource.JndiDbSource;
import org.sosostudio.dbking.exception.DbKingException;
import org.sosostudio.dbking.util.IoUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XmlConfig {

	private static final String CONFIG_FILENAME = "dbking.config.xml";

	private static boolean showSql = true;

	private static Map<String, DbSource> dbSourceMap = new ConcurrentHashMap<String, DbSource>();

	static {
		Document doc = null;
		InputStream is = null;
		try {
			is = XmlConfig.class.getResourceAsStream("/" + CONFIG_FILENAME);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			doc = db.parse(is);
		} catch (ParserConfigurationException e) {
			throw new DbKingException(e);
		} catch (SAXException e) {
			throw new DbKingException(e);
		} catch (IOException e) {
			throw new DbKingException(e);
		} finally {
			IoUtil.closeInputStream(is);
		}
		Element rootElement = doc.getDocumentElement();
		NodeList showSqlNodeList = rootElement.getElementsByTagName("show_sql");
		for (int i = 0; i < showSqlNodeList.getLength(); i++) {
			Element showSqlElement = (Element) showSqlNodeList.item(i);
			showSql = Boolean.valueOf(getOwnText(showSqlElement));
		}
		NodeList dbSourceNodeList = rootElement
				.getElementsByTagName("db_source");
		for (int i = 0; i < dbSourceNodeList.getLength(); i++) {
			Element dbSourceElement = (Element) dbSourceNodeList.item(i);
			String dbSourceName = dbSourceElement.getAttribute("name");
			String databaseDriver = null;
			String databaseUrl = null;
			String username = null;
			String password = null;
			String jndi = null;
			String schema = null;
			NodeList nodeList = dbSourceElement.getChildNodes();
			for (int j = 0; j < nodeList.getLength(); j++) {
				Node node = nodeList.item(j);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					String elementName = element.getNodeName();
					String value = getOwnText(element);
					if ("database_driver".equals(elementName)) {
						databaseDriver = value;
					} else if ("database_url".equals(elementName)) {
						databaseUrl = value;
					} else if ("username".equals(elementName)) {
						username = value;
					} else if ("password".equals(elementName)) {
						password = value;
					} else if ("jndi".equals(elementName)) {
						jndi = value;
					} else if ("schema".equals(elementName)) {
						schema = value;
					}
				}
			}
			if (databaseDriver != null && databaseUrl != null
					&& username != null && password != null) {
				DbSource dbSource = new JdbcDbSource(databaseDriver,
						databaseUrl, username, password, schema);
				dbSourceMap.put(dbSourceName, dbSource);
			} else if (databaseDriver != null && databaseUrl != null) {
				DbSource dbSource = new JdbcDbSource(databaseDriver,
						databaseUrl, schema);
				dbSourceMap.put(dbSourceName, dbSource);
			} else if (jndi != null && username != null && password != null) {
				DbSource dbSource = new JndiDbSource(jndi, username, password,
						schema);
				dbSourceMap.put(dbSourceName, dbSource);
			} else if (jndi != null) {
				DbSource dbSource = new JndiDbSource(jndi, schema);
				dbSourceMap.put(dbSourceName, dbSource);
			} else {
				throw new DbKingException("initial dbking config failed");
			}
		}
	}

	private static String getOwnText(Element element) {
		Node node = element.getFirstChild();
		return node == null ? null : node.getNodeValue();
	}

	public static boolean needsShowSql() {
		return showSql;
	}

	public static DbSource getDbSource(String name) {
		return dbSourceMap.get(name);
	}

}
