package org.sosostudio.dbunifier;

public class DbunifierException extends RuntimeException {

	private static final long serialVersionUID = 5238761288542684285L;

	public DbunifierException() {
	}

	public DbunifierException(String message) {
		super(message);
	}

	public DbunifierException(Throwable cause) {
		super(cause);
	}

	public DbunifierException(String message, Throwable cause) {
		super(message, cause);
	}

}
