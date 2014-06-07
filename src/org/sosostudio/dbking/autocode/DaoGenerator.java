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

package org.sosostudio.dbking.autocode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.sosostudio.dbking.DbKing;
import org.sosostudio.dbking.Table;
import org.sosostudio.dbking.util.IoUtil;

import freemarker.cache.StringTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class DaoGenerator {

	private static final String CONFIG_NAME = "autocode";

	private static String getTemplate(String templateFilename)
			throws IOException {
		InputStream is = DaoGenerator.class
				.getResourceAsStream(templateFilename);
		try {
			byte[] bytes = IoUtil.convertStream(is);
			return new String(bytes, "UTF-8");
		} finally {
			IoUtil.closeInputStream(is);
		}
	}

	public static void main(String[] args) throws TemplateException,
			IOException {
		Configuration cfg = new Configuration();
		StringTemplateLoader loader = new StringTemplateLoader();
		String infoTemplateSource = getTemplate("bean.ftl");
		loader.putTemplate("bean", infoTemplateSource);
		String daoTemplateSource = getTemplate("dao.ftl");
		loader.putTemplate("dao", daoTemplateSource);
		cfg.setTemplateLoader(loader);
		Template beanTemplate = cfg.getTemplate("bean", "UTF-8");
		Template daoTemplate = cfg.getTemplate("dao", "UTF-8");
		DbKing dbKing = new DbKing(CONFIG_NAME);
		String classPath = args[0];
		String packagePath = args[1];
		String destPath = classPath + "/" + packagePath.replaceAll("\\.", "/");
		new File(destPath).mkdirs();
		for (int i = 2; i < args.length; i++) {
			Map<String, Object> root = new HashMap<String, Object>();
			Table table = dbKing.getTable(args[i]);
			if (table == null) {
				table = dbKing.getView(args[i]);
			}
			root.put("package", packagePath);
			root.put("table", table);
			{
				Writer writer = null;
				try {
					writer = new FileWriter(destPath + "/"
							+ table.getDefinationName() + ".java");
					beanTemplate.process(root, writer);
					writer.flush();
				} finally {
					IoUtil.closeWriter(writer);
				}
			}
			{
				Writer writer = null;
				try {
					writer = new FileWriter(destPath + "/"
							+ table.getDefinationName() + "Dao.java");
					daoTemplate.process(root, writer);
					writer.flush();
				} finally {
					IoUtil.closeWriter(writer);
				}
			}
			System.out.println(table.getName()
					+ " BO and DAO classes have been generated");
		}

	}
}
