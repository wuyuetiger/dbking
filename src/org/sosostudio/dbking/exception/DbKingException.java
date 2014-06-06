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

package org.sosostudio.dbking.exception;

public class DbKingException extends RuntimeException {

	private static final long serialVersionUID = 5238761288542684285L;

	public DbKingException() {
	}

	public DbKingException(String message) {
		super(message);
	}

	public DbKingException(Throwable cause) {
		super(cause);
	}

	public DbKingException(String message, Throwable cause) {
		super(message, cause);
	}

}
