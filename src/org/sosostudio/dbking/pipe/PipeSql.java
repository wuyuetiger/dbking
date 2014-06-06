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

package org.sosostudio.dbking.pipe;

import java.util.ArrayList;
import java.util.List;

public class PipeSql {

	private String sql;

	private List<NameType> nameTypeList = new ArrayList<NameType>();

	public String getSql() {
		return sql;
	}

	public PipeSql setSql(String sql) {
		this.sql = sql;
		return this;
	}

	public List<NameType> getNameTypeList() {
		return nameTypeList;
	}

	public PipeSql addNameType(NameType nameType) {
		this.nameTypeList.add(nameType);
		return this;
	}

	public PipeSql addNameTypeList(List<NameType> nameTypeList) {
		this.nameTypeList.addAll(nameTypeList);
		return this;
	}

}
