package org.sosostudio.dbking.test;

import org.sosostudio.dbking.DbUnifier;

public class OracleTester extends BaseTester {

	public OracleTester() {
		unifier = new DbUnifier("oracle");
	}

}
