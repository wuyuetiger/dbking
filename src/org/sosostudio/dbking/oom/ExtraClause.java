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

import org.sosostudio.dbking.Values;

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
