package org.sosostudio.dbunifier;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class Values {

	private List<Object> valueList = new ArrayList<Object>();

	public List<Object> getValueList() {
		return valueList;
	}

	private Values addNullValue(String type) {
		NullValue nullValue = new NullValue(type);
		valueList.add(nullValue);
		return this;
	}

	public Values addStringValue(String stringValue) {
		if (stringValue == null) {
			return addNullValue(Column.TYPE_STRING);
		} else {
			valueList.add(stringValue);
			return this;
		}
	}

	public Values addNumberValue(BigDecimal numberValue) {
		if (numberValue == null) {
			return addNullValue(Column.TYPE_NUMBER);
		} else {
			valueList.add(numberValue);
			return this;
		}
	}

	public Values addDatetimeValue(Timestamp datetimeValue) {
		if (datetimeValue == null) {
			return addNullValue(Column.TYPE_DATETIME);
		} else {
			valueList.add(datetimeValue);
			return this;
		}
	}

	public Values addClobValue(String clobValue) {
		if (clobValue == null) {
			return addNullValue(Column.TYPE_CLOB);
		} else {
			valueList.add(clobValue.toCharArray());
			return this;
		}
	}

	public Values addBlobValue(byte[] blobValue) {
		if (blobValue == null) {
			return addNullValue(Column.TYPE_BLOB);
		} else {
			valueList.add(blobValue);
			return this;
		}
	}

	public Values addValues(Values values) {
		valueList.addAll(values.getValueList());
		return this;
	}

}
