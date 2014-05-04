package org.sosostudio.dbunifier;

import java.io.IOException;
import java.io.InputStream;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class DbXmlConfig {

	public static boolean showSql = true;

	public static DbConfig dbConfig = null;

	static {
		SAXReader saxReader = new SAXReader();
		InputStream is = null;
		try {
			is = DbXmlConfig.class
					.getResourceAsStream("/db-unifier.config.xml");
			Document doc = saxReader.read(is);
			Element rootElement = doc.getRootElement();
			showSql = Boolean.valueOf(rootElement.elementText("show_sql"));
			int mode = Integer.valueOf(rootElement.elementText("mode"));
			String databaseDriver = rootElement.elementText("database_driver");
			String databaseUrl = rootElement.elementText("database_url");
			String username = rootElement.elementText("username");
			String password = rootElement.elementText("password");
			String jndi = rootElement.elementText("jndi");
			switch (mode) {
			case 1: {
				dbConfig = new DbConfig(databaseDriver, databaseUrl, username,
						password);
				break;
			}
			case 2: {
				dbConfig = new DbConfig(databaseDriver, databaseUrl);
				break;
			}
			case 4: {
				dbConfig = new DbConfig(jndi);
				break;
			}
			case 6: {
				dbConfig = new DbConfig(jndi, username, password);
				break;
			}
			}
			if (dbConfig == null) {
				throw new DbUnifierException("initial db-unifier config failed");
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

}
