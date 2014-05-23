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

package org.sosostudio.dbunifier.autocode;

import java.util.ArrayList;

@SuppressWarnings("serial")
public class PaginationArrayList<V> extends ArrayList<V> {

	private int pageSize;

	private int pageNumber;

	private int totalCount;

	private int totalPageCount;

	public PaginationArrayList(int pageSize, int pageNumber, int totalCount) {
		if (pageSize > 0) {
			this.pageSize = pageSize;
		}
		if (pageNumber > 0) {
			this.pageNumber = pageNumber;
		}
		if (totalCount > 0) {
			this.totalCount = totalCount;
		}
		totalPageCount = (totalCount + pageSize - 1) / pageSize;
	}

	public int getPageSize() {
		return pageSize;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public int getTotalPageCount() {
		return totalPageCount;
	}

	public int getFirstPage() {
		return (totalPageCount == 0) ? 0 : 1;
	}

	public int getLastPage() {
		return totalPageCount;
	}

	public int getPrePage() {
		return (pageNumber > 1) ? pageNumber - 1 : 0;
	}

	public int getNextPage() {
		return (pageNumber < totalPageCount) ? pageNumber + 1 : 0;
	}

	public int[] getNeighbouringPage(int size) {
		int left = pageNumber - size;
		int right = pageNumber + size;
		int begin = left > 0 ? left : getFirstPage();
		int end = right < totalPageCount ? right : getLastPage();
		int[] num = new int[end - begin + 1];
		for (int i = 0; i < num.length; i++) {
			num[i] = begin + i;
		}
		return num;
	}

}
