package org.sosostudio.dbunifier.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

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

	public static void closeWriter(Writer writer) {
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				throw new DbUnifierException(e);
			}
		}
	}

	public static void convertStream(InputStream is, OutputStream os)
			throws IOException {
		byte[] buffer = new byte[2048];
		int bytesRead;
		while ((bytesRead = is.read(buffer, 0, 1024)) != -1) {
			os.write(buffer, 0, bytesRead);
		}
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

	public static void convertStream(Reader reader, Writer writer)
			throws IOException {
		char[] buffer = new char[2048];
		int charsRead;
		while ((charsRead = reader.read(buffer, 0, 1024)) != -1) {
			writer.write(buffer, 0, charsRead);
		}

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
