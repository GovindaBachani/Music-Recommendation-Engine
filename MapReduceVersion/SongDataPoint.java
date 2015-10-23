/* We represent each song as an Object of the SongDataPoint class
   implements the WritableComparable Interface*/
package org.mapreduce.kmeans.songdataset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SongDataPoint implements WritableComparable<SongDataPoint> {

	Text trackId;
	Text title;
	Text artistName;
	// these are the attributes we use to calculate Euclidean distances.
	DoubleArrayWritable attributesVector;

	public SongDataPoint() {
		this.trackId = new Text();
		this.title = new Text();
		this.artistName = new Text();
		this.attributesVector = new DoubleArrayWritable();
	}

	public SongDataPoint(Text trackId, Text title, Text artistName,
			DoubleArrayWritable attributesVector) {
		this.trackId = trackId;
		this.title = title;
		this.artistName = artistName;
		this.attributesVector = attributesVector;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.trackId.readFields(in);
		this.title.readFields(in);
		this.artistName.readFields(in);
		this.attributesVector.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.trackId.write(out);
		this.title.write(out);
		this.artistName.write(out);
		this.attributesVector.write(out);
	}

	public Text getTrackId() {
		return trackId;
	}

	public void setTrackId(Text trackId) {
		this.trackId = trackId;
	}

	public Text getTitle() {
		return title;
	}

	public void setTitle(Text title) {
		this.title = title;
	}

	public Text getArtistName() {
		return artistName;
	}

	public void setArtistName(Text artistName) {
		this.artistName = artistName;
	}

	public DoubleArrayWritable getAttributesVector() {
		return attributesVector;
	}

	public void setAttributesVector(DoubleArrayWritable attributesVector) {
		this.attributesVector = attributesVector;
	}

	@Override
	public int compareTo(SongDataPoint o) {
		int compare = getTrackId().compareTo(o.getTrackId());
		return compare;
	}

	@Override
	public int hashCode() {
		return trackId.hashCode() * 163 + artistName.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof SongDataPoint) {
			SongDataPoint tp = (SongDataPoint) o;
			return trackId.equals(tp.trackId)
					&& artistName.equals(tp.artistName);
		}
		return false;
	}

	@Override
	public String toString() {
		double[] attributes = this.attributesVector.getValueArray();
		StringBuilder sb = new StringBuilder();
		sb.append(this.trackId).append(':').append(this.title).append(':')
				.append(this.artistName).append(':')
				.append(Double.toString(attributes[0])).append(':')
				.append(Double.toString(attributes[1])).append(':')
				.append(Double.toString(attributes[2])).append(':')
				.append(Double.toString(attributes[3]));
		return sb.toString();
	}
}
