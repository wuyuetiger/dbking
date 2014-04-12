package org.sosostudio.dbunifier;

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
