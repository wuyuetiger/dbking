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
 *  https://git.oschina.net/db-unifier/db-unifier
 */

package org.sosostudio.dbking.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

public class IoUtil {

	public static void closeInputStream(InputStream is) {
		if (is != null) {
			try {
				is.close();
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeOutputStream(OutputStream os) {
		if (os != null) {
			try {
				os.close();
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeReader(Reader reader) {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeReader(XMLStreamReader reader) {
		if (reader != null) {
			try {
				reader.close();
			} catch (XMLStreamException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeReader(XMLEventReader reader) {
		if (reader != null) {
			try {
				reader.close();
			} catch (XMLStreamException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeWriter(Writer writer) {
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void closeWriter(XMLStreamWriter writer) {
		if (writer != null) {
			try {
				writer.close();
			} catch (XMLStreamException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static int convertStream(InputStream is, OutputStream os)
			throws IOException {
		int count = 0;
		byte[] buffer = new byte[2048];
		int bytesRead;
		while ((bytesRead = is.read(buffer, 0, 1024)) != -1) {
			os.write(buffer, 0, bytesRead);
			count += bytesRead;
		}
		return count;
	}

	public static byte[] convertStream(InputStream is) throws IOException {
		if (is == null) {
			return null;
		} else {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			convertStream(is, baos);
			return baos.toByteArray();
		}
	}

	public static int convertStream(Reader reader, Writer writer)
			throws IOException {
		int count = 0;
		char[] buffer = new char[2048];
		int charsRead;
		while ((charsRead = reader.read(buffer, 0, 1024)) != -1) {
			writer.write(buffer, 0, charsRead);
			count += charsRead;
		}
		return count;
	}

	public static String convertStream(Reader reader) throws IOException {
		if (reader == null) {
			return null;
		} else {
			StringWriter writer = new StringWriter();
			convertStream(reader, writer);
			return writer.toString();
		}
	}

}
