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
