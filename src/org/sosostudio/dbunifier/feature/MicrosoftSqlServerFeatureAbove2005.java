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
 *  https://git.oschina.net/db-unifier/db-unifier
 */

package org.sosostudio.dbunifier.feature;

public class MicrosoftSqlServerFeatureAbove2005 extends
		MicrosoftSqlServerFeatureBelow2005 {

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		if ("".equals(orderBySubSql)) {
			orderBySubSql = "order by (select 1)";
		}
		sb.append("select * from ( select tmp1_.*, row_number() over (")
				.append(orderBySubSql).append(") rownum_ from ( ")
				.append(mainSubSql).append(" ) tmp1_ ) tmp2_ where rownum_ > ")
				.append(startPos).append(" and rownum_ <= ").append(endPos);
		return sb.toString();
	}

}
