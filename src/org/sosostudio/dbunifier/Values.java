package org.sosostudio.dbunifier;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class Values {

	private List<Object> valueList = new ArrayList<Object>();

	public List<Object> getValueList() {
		return this.valueList;
	}

	private Values addNullValue(String type) {
		NullValue nullValue = new NullValue(type);
		this.valueList.add(nullValue);
		return this;
	}

	public Values addStringValue(String stringValue) {
		if (stringValue == null) {
			return this.addNullValue(Column.TYPE_STRING);
		} else {
			this.valueList.add(stringValue);
			return this;
		}
	}

	public Values addNumberValue(BigDecimal numberValue) {
		if (numberValue == null) {
			return this.addNullValue(Column.TYPE_NUMBER);
		} else {
			this.valueList.add(numberValue);
			return this;
		}
	}

	public Values addDatetimeValue(Timestamp datetimeValue) {
		if (datetimeValue == null) {
			return this.addNullValue(Column.TYPE_DATETIME);
		} else {
			this.valueList.add(datetimeValue);
			return this;
		}
	}

	public Values addClobValue(char[] clobValue) {
		if (clobValue == null) {
			return this.addNullValue(Column.TYPE_CLOB);
		} else {
			this.valueList.add(clobValue);
			return this;
		}
	}

	public Values addBlobValue(byte[] blobValue) {
		if (blobValue == null) {
			return this.addNullValue(Column.TYPE_BLOB);
		} else {
			this.valueList.add(blobValue);
			return this;
		}
	}

	public Values addValues(Values values) {
		this.valueList.addAll(values.getValueList());
		return this;
	}

}
