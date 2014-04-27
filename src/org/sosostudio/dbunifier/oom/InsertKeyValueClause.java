package org.sosostudio.dbunifier.oom;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.sosostudio.dbunifier.Values;

public class InsertKeyValueClause {

	private StringBuilder keysSb = new StringBuilder();

	private StringBuilder valuesSb = new StringBuilder();

	private Values values = new Values();

	public InsertKeyValueClause addStringClause(String columnName,
			String stringValue) {
		if (keysSb.length() > 0) {
			keysSb.append(", ");
			valuesSb.append(", ");
		}
		keysSb.append(columnName);
		if (stringValue == null) {
			valuesSb.append("null");
		} else {
			valuesSb.append("?");
			values.addStringValue(stringValue);
		}
		return this;
	}

	public InsertKeyValueClause addNumberClause(String columnName,
			BigDecimal numberValue) {
		if (keysSb.length() > 0) {
			keysSb.append(", ");
			valuesSb.append(", ");
		}
		keysSb.append(columnName);
		if (numberValue == null) {
			valuesSb.append("null");
		} else {
			valuesSb.append("?");
			values.addNumberValue(numberValue);
		}
		return this;
	}

	public InsertKeyValueClause addTimestampClause(String columnName,
			Timestamp timestampValue) {
		if (keysSb.length() > 0) {
			keysSb.append(", ");
			valuesSb.append(", ");
		}
		keysSb.append(columnName);
		if (timestampValue == null) {
			valuesSb.append("null");
		} else {
			valuesSb.append("?");
			values.addTimestampValue(timestampValue);
		}
		return this;
	}

	public InsertKeyValueClause addClobClause(String columnName,
			String clobValue) {
		if (keysSb.length() > 0) {
			keysSb.append(", ");
			valuesSb.append(", ");
		}
		keysSb.append(columnName);
		if (clobValue == null) {
			valuesSb.append("null");
		} else {
			valuesSb.append("?");
			values.addClobValue(clobValue);
		}
		return this;
	}

	public InsertKeyValueClause addBlobClause(String columnName,
			byte[] blobValue) {
		if (keysSb.length() > 0) {
			keysSb.append(", ");
			valuesSb.append(", ");
		}
		keysSb.append(columnName);
		if (blobValue == null) {
			valuesSb.append("null");
		} else {
			valuesSb.append("?");
			values.addBlobValue(blobValue);
		}
		return this;
	}

	public String getKeysClause() {
		return keysSb.toString();
	}

	public String getValuesClause() {
		return valuesSb.toString();
	}

	public Values getValues() {
		return values;
	}

}
