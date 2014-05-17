/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2014 YU YUE, SOSO STUDIO, wuyuetiger@gmail.com
 *
 * License: GNU Lesser General Public License (LGPL)
 * 
 * Source code availability:
 *  https://github.com/wuyuetiger/db-unifier
 *  https://code.csdn.net/tigeryu/db-unifier
 */

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
