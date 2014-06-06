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

package org.sosostudio.dbking.feature;

public class Db2FeatureBelow972 extends DbFeature {

	@Override
	public String getPaginationSql(String mainSubSql, String orderBySubSql,
			int startPos, int endPos) {
		StringBuilder sb = new StringBuilder();
		sb.append(
				"select * from ( select tmp1_.*, row_number() over() as rownum_ from ( ")
				.append(mainSubSql).append(orderBySubSql)
				.append(" ) tmp1_ ) tmp2_ where rownum_ > ").append(startPos)
				.append(" and rownum_ <= ").append(endPos);
		return sb.toString();
	}

}
