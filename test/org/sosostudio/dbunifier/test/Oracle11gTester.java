package org.sosostudio.dbunifier.test;

import org.junit.Test;
import org.sosostudio.dbunifier.DbUnifier;
import org.sosostudio.dbunifier.util.DbUnifierException;

public class Oracle11gTester extends BaseTester {

	public Oracle11gTester() {
		unifier = new DbUnifier("oracle");
	}

	@Test
	public void testLongVarchar() {
		throw new DbUnifierException("long varchar will be recognized as long");
	}

}