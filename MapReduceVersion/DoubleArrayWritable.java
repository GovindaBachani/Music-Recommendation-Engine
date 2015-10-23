/* This is the custom ComparapleWritable class which we can write into HDFS 
 * output Files, inherits from the ArrayWritable and implements the 
 * WritableComparable Interface*/

package org.mapreduce.kmeans.songdataset;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class DoubleArrayWritable extends ArrayWritable implements
		WritableComparable {

	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}

	@Override
	public int compareTo(Object o) {
		if (this.equals(o)) {
			return 0;
		} else {
			return 1;
		}
	}

	public double[] getValueArray() {
		Writable[] wValues = get();
		double[] values = new double[wValues.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = ((DoubleWritable) wValues[i]).get();
		}
		return values;
	}

	public void setValueArray(double[] values) {
		Writable[] wValues = new DoubleWritable[values.length];
		for (int i = 0; i < values.length; i++) {
			wValues[i] = new DoubleWritable(values[i]);
		}
		set(wValues);
	}

	public String toString() {
		double[] values = getValueArray();

		StringBuilder strBuilder = new StringBuilder();

		StringBuilder breaker = new StringBuilder();
		for (int i = 0; i < values.length; ++i) {
			strBuilder.append(breaker);
			strBuilder.append(String.format("%.4f", values[i]));
			breaker.setLength(1);
			breaker.setCharAt(0, ',');
		}

		return strBuilder.toString();
	}

	public boolean equals(Object obj) {
		DoubleArrayWritable other = (DoubleArrayWritable) obj;
		Writable[] otherWValues = other.get();
		Writable[] thisWValues = get();
		double a[] =this.getValueArray();
		double b[] =other.getValueArray();

		if (otherWValues.length != thisWValues.length) {
			return false;
		}

		for (int i = 0; i < thisWValues.length; ++i) {
			double diff = Math.abs(a[i]-b[i]);
			if (diff != 0) {
				return false;
			}
		}
		return true;
	}
	
	public double distance(DoubleArrayWritable dar){
		double square = 0;
		double [] myAttribute = this.getValueArray();
		double [] darAttribute = dar.getValueArray();
		for(int i = 0; i<=3;i++){
			square = square +(myAttribute[i]-darAttribute[i])*(myAttribute[i]-darAttribute[i]);
		}
		double distance = Math.sqrt(square);
		
		return distance;
	}
}
