package org.sosostudio.dbunifier.config;

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
import org.sosostudio.dbunifier.util.IoUtil;

public class XmlConfig {

	private static boolean showSql = true;

	private static Map<String, DbSource> dbSourceMap = new ConcurrentHashMap<String, DbSource>();

	static {
		SAXReader saxReader = new SAXReader();
		InputStream is = null;
		try {
			is = XmlConfig.class.getResourceAsStream("/db-unifier.config.xml");
			Document doc = saxReader.read(is);
			Element rootElement = doc.getRootElement();
			showSql = Boolean.valueOf(rootElement.elementText("show_sql"));
			@SuppressWarnings("unchecked")
			List<Element> dbSourceElementList = rootElement
					.elements("db_source");
			for (Element dbSourceElement : dbSourceElementList) {
				String dbSourceName = dbSourceElement.attributeValue("name");
				if (dbSourceName == null) {
					dbSourceName = "";
				}
				String databaseDriver = dbSourceElement
						.elementText("database_driver");
				String databaseUrl = dbSourceElement
						.elementText("database_url");
				String username = dbSourceElement.elementText("username");
				String password = dbSourceElement.elementText("password");
				String jndi = dbSourceElement.elementText("jndi");
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
			IoUtil.closeInputStream(is);
		}
	}

	public static boolean needsShowSql() {
		return showSql;
	}

	public static DbSource getDbSource(String name) {
		return dbSourceMap.get(name);
	}

}
