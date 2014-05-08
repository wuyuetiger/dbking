package org.sosostudio.dbunifier.autocode;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.DbUnifierException;
import org.sosostudio.dbunifier.Table;

import freemarker.cache.StringTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class DaoGenerator {

	private static String getTemplate(String templateFilename)
			throws IOException, UnsupportedEncodingException {
		InputStream is = DaoGenerator.class
				.getResourceAsStream(templateFilename);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buffer = new byte[2048];
		int bytesRead;
		try {
			while ((bytesRead = is.read(buffer, 0, 1024)) != -1) {
				baos.write(buffer, 0, bytesRead);
			}
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
		}
		return new String(baos.toByteArray(), "UTF-8");
	}

	public static void main(String[] args) throws TemplateException,
			IOException, UnsupportedEncodingException {
		Configuration cfg = new Configuration();
		StringTemplateLoader loader = new StringTemplateLoader();
		String infoTemplateSource = getTemplate("bean.ftl");
		loader.putTemplate("bean", infoTemplateSource);
		String daoTemplateSource = getTemplate("dao.ftl");
		loader.putTemplate("dao", daoTemplateSource);
		cfg.setTemplateLoader(loader);
		Template beanTemplate = cfg.getTemplate("bean", "UTF-8");
		Template daoTemplate = cfg.getTemplate("dao", "UTF-8");
		DbUnifier unifier = new DbUnifier();
		String classPath = args[0];
		String packagePath = args[1];
		String destPath = classPath + "/" + packagePath.replaceAll("\\.", "/");
		new File(destPath).mkdirs();
		for (int i = 2; i < args.length; i++) {
			Map<String, Object> root = new HashMap<String, Object>();
			Table table = unifier.getTable(args[i]);
			root.put("package", packagePath);
			root.put("table", table);
			{
				Writer out = new FileWriter(destPath + "/"
						+ table.getDefinationName() + ".java");
				beanTemplate.process(root, out);
				out.flush();
				out.close();
			}
			{
				Writer out = new FileWriter(destPath + "/"
						+ table.getDefinationName() + "Dao.java");
				daoTemplate.process(root, out);
				out.flush();
				out.close();
			}
			System.out.println(table.getName()
					+ " DAO classes had been generated");
		}

	}

}
