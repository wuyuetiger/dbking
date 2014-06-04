package org.sosostudio.dbunifier.sample;

import org.sosostudio.dbunifier.DbUnifier;

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
				DbUnifier unifier = new DbUnifier();
				for (int i = 0; i < Integer.MAX_VALUE; i++) {
					System.out.println("***** " + name + " value = "
							+ unifier.getSequenceNextValue("test"));
					try {
						Thread.sleep(delay);
					} catch (InterruptedException ex) {
					}
				}
			}
		}, name).start();
	}

}
