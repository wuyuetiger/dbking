package org.sosostudio.dbunifier.oom;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.sosostudio.dbunifier.Values;

public class UpdateKeyValueClause {

	private StringBuilder sb = new StringBuilder();

	private Values values = new Values();

	public UpdateKeyValueClause addStringClause(String columnName,
			String stringValue) {
		if (sb.length() > 0) {
			sb.append(", ");
		}
		sb.append(columnName).append(" = ");
		if (stringValue == null) {
			sb.append("null");
		} else {
			sb.append("?");
			values.addStringValue(stringValue);
		}
		return this;
	}

	public UpdateKeyValueClause addNumberClause(String columnName,
			BigDecimal numberValue) {
		if (sb.length() > 0) {
			sb.append(", ");
		}
		sb.append(columnName).append(" = ");
		if (numberValue == null) {
			sb.append("null");
		} else {
			sb.append("?");
			values.addNumberValue(numberValue);
		}
		return this;
	}

	public UpdateKeyValueClause addTimestampClause(String columnName,
			Timestamp timestampValue) {
		if (sb.length() > 0) {
			sb.append(", ");
		}
		sb.append(columnName).append(" = ");
		if (timestampValue == null) {
			sb.append("null");
		} else {
			sb.append("?");
			values.addTimestampValue(timestampValue);
		}
		return this;
	}

	public UpdateKeyValueClause addClobClause(String columnName,
			String clobValue) {
		if (sb.length() > 0) {
			sb.append(", ");
		}
		sb.append(columnName).append(" = ");
		if (clobValue == null) {
			sb.append("null");
		} else {
			sb.append("?");
			values.addClobValue(clobValue);
		}
		return this;
	}

	public UpdateKeyValueClause addBlobClause(String columnName,
			byte[] blobValue) {
		if (sb.length() > 0) {
			sb.append(", ");
		}
		sb.append(columnName).append(" = ");
		if (blobValue == null) {
			sb.append("null");
		} else {
			sb.append("?");
			values.addBlobValue(blobValue);
		}
		return this;
	}

	public String getClause() {
		return sb.toString();
	}

	public Values getValues() {
		return values;
	}

}
