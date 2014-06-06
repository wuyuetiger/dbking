package org.sosostudio.dbking.sample;

import org.sosostudio.dbking.DbKing;
import org.sosostudio.dbking.exception.DbKingException;

public class SequenceSample {

	public static void main(String[] args) {
		startTestThread("Thread #1 ", (int) (10 * Math.random()));
		startTestThread("Thread #2 ", (int) (10 * Math.random()));
		startTestThread("Thread #3 ", (int) (10 * Math.random()));
		startTestThread("Thread #4 ", (int) (10 * Math.random()));
		startTestThread("Thread #5 ", (int) (10 * Math.random()));
	}

	private static void startTestThread(final String name, final int delay) {
		new Thread(new Runnable() {
			public void run() {
				DbKing dbKing = new DbKing();
				for (int i = 0; i < Integer.MAX_VALUE; i++) {
					long seq;
					try {
						seq = dbKing.getSequenceNextValue("test");
					} catch (DbKingException e) {
						continue;
					}
					System.out.println("***** " + name + " value = " + seq);
					try {
						Thread.sleep(delay);
					} catch (InterruptedException ex) {
					}
				}
			}
		}, name).start();
	}

}
