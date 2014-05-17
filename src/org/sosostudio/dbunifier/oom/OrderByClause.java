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

public class OrderByClause {

	private StringBuilder sb = new StringBuilder();

	public OrderByClause addOrder(String columnName, Direction direction) {
		if (sb.length() > 0) {
			sb.append(", ");
		}
		sb.append(columnName).append(" ");
		if (direction == Direction.ASC) {
			sb.append("asc");
		} else if (direction == Direction.DESC) {
			sb.append("desc");
		}
		return this;
	}

	public String getClause() {
		return sb.toString();
	}

}
