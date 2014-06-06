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

public class DbUnifierException extends RuntimeException {

	private static final long serialVersionUID = 5238761288542684285L;

	public DbUnifierException() {
	}

	public DbUnifierException(String message) {
		super(message);
	}

	public DbUnifierException(Throwable cause) {
		super(cause);
	}

	public DbUnifierException(String message, Throwable cause) {
		super(message, cause);
	}

}
