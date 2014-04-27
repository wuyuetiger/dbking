package org.sosostudio.dbunifier.oom;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;

import org.sosostudio.dbunifier.Values;

public class ConditionClause {

	private StringBuilder sb = new StringBuilder();

	private Values values = new Values();

	private LogicalOp logicalOp;

	public ConditionClause(LogicalOp logicalOp) {
		this.logicalOp = logicalOp;
	}

	private ConditionClause addClause(String columnName, RelationOp relationOp,
			Object value) {
		if (sb.length() > 0) {
			if (logicalOp == LogicalOp.AND) {
				sb.append(" and ");
			} else if (logicalOp == LogicalOp.OR) {
				sb.append(" or ");
			}
		}
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

	private ConditionClause addClause(String columnName, SetOp setOp,
			Collection<? extends Object> collection) {
		if (sb.length() > 0) {
			if (logicalOp == LogicalOp.AND) {
				sb.append(" and");
			} else if (logicalOp == LogicalOp.OR) {
				sb.append(" or");
			}
		}
		sb.append(columnName).append(" ");
		if (setOp == SetOp.IN) {
			if (collection != null) {
				if (collection.size() == 0) {
					sb.append("1 = 2");
				} else {
					sb.append("in (");
				}
			}
		} else if (setOp == SetOp.NOT_IN) {
			if (collection != null) {
				if (collection.size() == 0) {
					sb.append("1 = 1");
				} else {
					sb.append("not in (");
				}
			}
		}
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
		return this;
	}

	public ConditionClause addStringClause(String columnName,
			RelationOp relationOp, String stringValue) {
		return addClause(columnName, relationOp, stringValue);
	}

	public ConditionClause addNumberClause(String columnName,
			RelationOp relationOp, BigDecimal numberValue) {
		return addClause(columnName, relationOp, numberValue);
	}

	public ConditionClause addTimestampClause(String columnName,
			RelationOp relationOp, Timestamp timestampValue) {
		return addClause(columnName, relationOp, timestampValue);
	}

	public ConditionClause addStringClause(String columnName, SetOp setOp,
			Collection<String> stringCollection) {
		return addClause(columnName, setOp, stringCollection);
	}

	public ConditionClause addNumberClause(String columnName, SetOp setOp,
			Collection<BigDecimal> numberCollection) {
		return addClause(columnName, setOp, numberCollection);
	}

	public ConditionClause addTimestampClause(String columnName, SetOp setOp,
			Collection<Timestamp> timestampCollection) {
		return addClause(columnName, setOp, timestampCollection);
	}

	public String getClause() {
		return sb.toString();
	}

	public Values getValues() {
		return values;
	}

}
