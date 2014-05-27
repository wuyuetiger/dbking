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

package org.sosostudio.dbunifier.util;

import java.io.UnsupportedEncodingException;

import org.sosostudio.dbunifier.ColumnType;
import org.sosostudio.dbunifier.Encoding;

public class StringUtil {

	private static String getName(String name, boolean first) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < name.length(); i++) {
			char ch = name.charAt(i);
			if (ch == '_') {
				first = true;
				continue;
			}
			if (first) {
				sb.append(Character.toUpperCase(ch));
				first = false;
			} else {
				sb.append(Character.toLowerCase(ch));
			}
		}
		return sb.toString();
	}

	public static String getDefinationName(String name) {
		return getName(name, true);
	}

	public static String getVariableName(String name) {
		return getName(name, false);
	}

	public static String substring(String s, int maxSize, Encoding encoding) {
		if (s == null) {
			return null;
		}
		if (encoding == Encoding.UTF8) {
			try {
				byte[] bytes = s.getBytes("UTF-8");
				if (bytes.length <= maxSize) {
					return s;
				} else {
					if (maxSize >= ColumnType.MAX_STRING_SIZE) {
						StringBuilder sb = new StringBuilder();
						int length = 0;
						for (int i = 0; i < s.length(); i++) {
							String ch = s.substring(i, i + 1);
							length += ch.getBytes("UTF-8").length;
							if (length > maxSize) {
								break;
							}
							sb.append(ch);
						}
						return sb.toString();
					} else {
						throw new DbUnifierException("string is too long");
					}
				}
			} catch (UnsupportedEncodingException e) {
				throw new DbUnifierException(e);
			}
		} else if (encoding == Encoding.GBK) {
			try {
				byte[] bytes = s.getBytes("GBK");
				if (bytes.length <= maxSize) {
					return s;
				} else {
					if (maxSize >= ColumnType.MAX_STRING_SIZE) {
						StringBuilder sb = new StringBuilder();
						int length = 0;
						for (int i = 0; i < s.length(); i++) {
							String ch = s.substring(i, i + 1);
							length += ch.getBytes("GBK").length;
							if (length > maxSize) {
								break;
							}
							sb.append(ch);
						}
						return sb.toString();
					} else {
						throw new DbUnifierException("string is too long");
					}
				}
			} catch (UnsupportedEncodingException e) {
				throw new DbUnifierException(e);
			}
		} else {
			if (s.length() <= maxSize) {
				return s;
			} else {
				if (maxSize >= ColumnType.MAX_STRING_SIZE) {
					return s.substring(0, maxSize);
				} else {
					throw new DbUnifierException("string is too long");
				}
			}
		}
	}

}
