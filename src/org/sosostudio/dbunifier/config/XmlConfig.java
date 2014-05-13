package org.sosostudio.dbunifier.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.sosostudio.dbunifier.dbsource.DbSource;
import org.sosostudio.dbunifier.dbsource.JdbcDbSource;
import org.sosostudio.dbunifier.dbsource.JndiDbSource;
import org.sosostudio.dbunifier.util.DbUnifierException;

public class XmlConfig {

	private static boolean showSql = true;

	private static Map<String, DbSource> dbSourceMap = new ConcurrentHashMap<String, DbSource>();

	static {
		SAXReader saxReader = new SAXReader();
		InputStream is = null;
		try {
			is = XmlConfig.class
					.getResourceAsStream("/db-unifier.config.xml");
			Document doc = saxReader.read(is);
			Element rootElement = doc.getRootElement();
			showSql = Boolean.valueOf(rootElement.elementText("show_sql"));
			List<Element> dbSourceElementList = rootElement
					.elements("db_source");
			for (Element dbSourceElement : dbSourceElementList) {
				String dbSourceName = dbSourceElement.attributeValue("name");
				String databaseDriver = rootElement
						.elementText("database_driver");
				String databaseUrl = rootElement.elementText("database_url");
				String username = rootElement.elementText("username");
				String password = rootElement.elementText("password");
				String jndi = rootElement.elementText("jndi");
				if (databaseDriver != null && databaseUrl != null
						&& username != null && password != null) {
					DbSource dbSource = new JdbcDbSource(databaseDriver,
							databaseUrl, username, password);
					dbSourceMap.put(dbSourceName, dbSource);
				} else if (databaseDriver != null && databaseUrl != null) {
					DbSource dbSource = new JdbcDbSource(databaseDriver,
							databaseUrl);
					dbSourceMap.put(dbSourceName, dbSource);
				} else if (jndi != null && username != null && password != null) {
					DbSource dbSource = new JndiDbSource(jndi, username,
							password);
					dbSourceMap.put(dbSourceName, dbSource);
				} else if (jndi != null) {
					DbSource dbSource = new JndiDbSource(jndi);
					dbSourceMap.put(dbSourceName, dbSource);
				} else {
					throw new DbUnifierException(
							"initial db-unifier config failed");
				}

			}
		} catch (DocumentException e) {
			throw new DbUnifierException(e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					throw new DbUnifierException(e);
				}
			}
		}
	}

	public static boolean needsShowSql() {
		return showSql;
	}

	public static DbSource getDbSource(String name) {
		return dbSourceMap.get(name);
	}

}
