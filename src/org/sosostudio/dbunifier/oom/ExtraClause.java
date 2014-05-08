package org.sosostudio.dbunifier.oom;

import org.sosostudio.dbunifier.Values;

public class ExtraClause {

	private String clause = "";

	private Values values = new Values();

	public String getClause() {
		return clause;
	}

	public ExtraClause setClause(String clause) {
		this.clause = clause;
		return this;
	}

	public Values getValues() {
		return values;
	}

	public ExtraClause setValues(Values values) {
		this.values = values;
		return this;
	}

}
