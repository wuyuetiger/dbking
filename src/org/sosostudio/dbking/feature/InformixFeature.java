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

package org.sosostudio.dbking.feature;

import java.math.BigInteger;
import java.sql.DatabaseMetaData;
import java.util.UUID;

public class InformixFeature extends DbFeature {

	public InformixFeature(DatabaseMetaData dmd) {
		super(dmd);
	}

	@Override
	public String getTimestampDbType() {
		return "datetime year to fraction";
	}

	@Override
	public String defaultCaps(String name) {
		return name.toLowerCase();
	}

	@Override
	public String constraintClause(StringBuilder sbPk) {
		StringBuilder sb = new StringBuilder();
		String uuid16 = UUID.randomUUID().toString().replace("-", "");
		BigInteger big = new BigInteger(uuid16, 16);
		String uuid36 = big.toString(36);
		sb.append(", primary key (").append(sbPk).append(") constraint ")
				.append("PK_").append(uuid36);
		return sb.toString();
	}

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append("select skip ").append(startPos).append(" first ")
				.append(endPos - startPos).append(" * from (")
				.append(mainSubSql).append(orderBySubSql).append(")");
		return sb.toString();
	}

}
