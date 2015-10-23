/* The data we are processing is buggy. So sometimes where we expect a number
 * we don't get it there. We throw a custom defined Exception in this case.*/

package org.mapreduce.kmeans.songdataset;

public class NaNException extends Exception {

	private static final long serialVersionUID = 1L;

	public NaNException() {
	}

	// Constructor that accepts a message
	public NaNException(String message) {
		super(message);
	}
}
