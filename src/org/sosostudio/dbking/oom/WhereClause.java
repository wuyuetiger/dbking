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

package org.sosostudio.dbking.oom;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.sosostudio.dbking.Values;

public class WhereClause {

	private StringBuilder sb = new StringBuilder();

	private Values values = new Values();

	private LogicalOp logicalOp;

	public WhereClause(LogicalOp logicalOp) {
		this.logicalOp = logicalOp;
	}

	private void setLogicalOp() {
		if (sb.length() > 0) {
			if (logicalOp == LogicalOp.AND) {
				sb.append(" and ");
			} else if (logicalOp == LogicalOp.OR) {
				sb.append(" or ");
			}
		}
	}

	private WhereClause addClause(String columnName, RelationOp relationOp,
			Object value) {
		setLogicalOp();
		sb.append(columnName).append(" ");
		if (relationOp == RelationOp.EQUAL) {
			if (value == null) {
				sb.append("is null");
			} else {
				sb.append("= ?");
				values.getValueList().add(value);
			}
		} else if (relationOp == RelationOp.UNEQUAL) {
			if (value == null) {
				sb.append("is not null");
			} else {
				sb.append("= ?");
				values.getValueList().add(value);
			}
		} else if (relationOp == RelationOp.GREATER) {
			if (value != null) {
				sb.append("> ?");
				values.getValueList().add(value);
			}
		} else if (relationOp == RelationOp.GREATER_EQUAL) {
			if (value != null) {
				sb.append(">= ?");
				values.getValueList().add(value);
			}
		} else if (relationOp == RelationOp.LESS) {
			if (value != null) {
				sb.append("< ?");
				values.getValueList().add(value);
			}
		} else if (relationOp == RelationOp.LESS_EQUAL) {
			if (value != null) {
				sb.append("<= ?");
				values.getValueList().add(value);
			}
		} else if (relationOp == RelationOp.LIKE) {
			if (value == null) {
				sb.append("is null");
			} else {
				sb.append("like ?");
				values.getValueList().add("%" + value + "%");
			}
		} else if (relationOp == RelationOp.NOT_LIKE) {
			if (value == null) {
				sb.append("is not null");
			} else {
				sb.append("not like ?");
				values.getValueList().add("%" + value + "%");
			}
		} else if (relationOp == RelationOp.ORIGINAL_LIKE) {
			if (value == null) {
				sb.append("is null");
			} else {
				sb.append("like ?");
				values.getValueList().add(value);
			}
		} else if (relationOp == RelationOp.NOT_ORIGINAL_LIKE) {
			if (value == null) {
				sb.append("is not null");
			} else {
				sb.append("not like ?");
				values.getValueList().add(value);
			}
		}
		return this;
	}

	private WhereClause addClause(String columnName, SetOp setOp,
			Collection<? extends Object> collection) {
		if (collection == null) {
			collection = new HashSet<Object>();
		}
		setLogicalOp();
		sb.append(columnName).append(" ");
		if (setOp == SetOp.IN) {

			if (collection.size() == 0) {
				sb.append("1 = 2");
			} else {
				sb.append("in (");
			}
		} else if (setOp == SetOp.NOT_IN) {
			if (collection.size() == 0) {
				sb.append("1 = 1");
			} else {
				sb.append("not in (");
			}
		}
		if (collection.size() > 0) {
			Iterator<?> it = collection.iterator();
			if (it.hasNext()) {
				sb.append("?");
				values.getValueList().add(it.next());
			}
			while (it.hasNext()) {
				sb.append(", ?");
				values.getValueList().add(it.next());
			}
			sb.append(")");
		}
		return this;
	}

	public WhereClause addStringClause(String columnName,
			RelationOp relationOp, String stringValue) {
		return addClause(columnName, relationOp, stringValue);
	}

	public WhereClause addNumberClause(String columnName,
			RelationOp relationOp, BigDecimal numberValue) {
		return addClause(columnName, relationOp, numberValue);
	}

	public WhereClause addTimestampClause(String columnName,
			RelationOp relationOp, Timestamp timestampValue) {
		return addClause(columnName, relationOp, timestampValue);
	}

	public WhereClause addStringClause(String columnName, SetOp setOp,
			Collection<String> stringCollection) {
		return addClause(columnName, setOp, stringCollection);
	}

	public WhereClause addNumberClause(String columnName, SetOp setOp,
			Collection<BigDecimal> numberCollection) {
		return addClause(columnName, setOp, numberCollection);
	}

	public WhereClause addTimestampClause(String columnName, SetOp setOp,
			Collection<Timestamp> timestampCollection) {
		return addClause(columnName, setOp, timestampCollection);
	}

	public WhereClause addClause(WhereClause whereClause) {
		setLogicalOp();
		sb.append("(").append(whereClause.toString()).append(")");
		values.addValues(whereClause.getValues());
		return this;
	}

	public WhereClause addClause(String clause, Values values) {
		setLogicalOp();
		sb.append(clause);
		values.addValues(values);
		return this;
	}

	public String getClause() {
		return sb.toString();
	}

	public Values getValues() {
		return values;
	}

}
