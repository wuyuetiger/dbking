package org.sosostudio.dbunifier.pipe;

import java.io.StringWriter;

public class Tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
//		DbExporter.main(new String[]{});
		DbImporter.main(new String[]{});
		long end = System.currentTimeMillis();
		System.out.println(end - start);
	}

}
