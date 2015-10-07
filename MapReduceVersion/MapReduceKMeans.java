package org.mapreduce.kmeans.songdataset;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduceKMeans {

	private enum COUNTERS {
		LIMIT
	}

	public static class RandomSamplingMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		Random rands = new Random();
		double percentage;

		@Override
		protected void setup(
				Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String strPercentage = context.getConfiguration().get("percentage");
			percentage = Double.parseDouble(strPercentage) / 100;
		}

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String split[] = str.split(",");

			double[] attributes = new double[4];
			attributes[0] = Double.parseDouble(split[3]);
			attributes[1] = Double.parseDouble(split[4]);
			attributes[2] = Double.parseDouble(split[5]);
			attributes[3] = Double.parseDouble(split[6]);

			DoubleArrayWritable centroid = new DoubleArrayWritable();
			centroid.setValueArray(attributes);

			Text dar = new Text(centroid.toString());

			long count = context.getCounter(COUNTERS.LIMIT).getValue();
			long limit = Long
					.parseLong(context.getConfiguration().get("limit"));
			if (rands.nextDouble() < percentage && count < limit) {
				context.write(dar, NullWritable.get());
				context.getCounter(COUNTERS.LIMIT).increment(1);
			}

		}
	}

	public static class MapReduceKMeansMapper extends
			Mapper<Object, Text, Text, SongDataPoint> {
		ArrayList<DoubleArrayWritable> centroids;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			centroids = new ArrayList<DoubleArrayWritable>();
			Configuration conf = context.getConfiguration();

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			ArrayList<Path> paths = new ArrayList<Path>();
			for (Path cacheFile : cacheFiles) {
				paths.add(cacheFile);
			}
			try {
				for (Path p : paths) {
					BufferedReader readBuffer1 = new BufferedReader(
							new FileReader(p.toString()));
					String line;
					while ((line = readBuffer1.readLine()) != null) {
						String[] split = line.split(",");
						double centroid[] = new double[4];
						centroid[0] = Double.parseDouble(split[0]);
						centroid[1] = Double.parseDouble(split[1]);
						centroid[2] = Double.parseDouble(split[2]);
						centroid[3] = Double.parseDouble(split[3]);
						DoubleArrayWritable centr = new DoubleArrayWritable();
						centr.setValueArray(centroid);
						centroids.add(centr);
					}
					readBuffer1.close();
				}
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}

		public void map(Object key, Text value, Context context)
				throws InterruptedException, IOException {
			try {
				String str = value.toString();
				String split[] = str.split(",");

				String trackId = split[0];
				String title = split[1];
				String artistName = split[2];
				double[] attributes = new double[4];
				attributes[0] = Double.parseDouble(split[3]);
				attributes[1] = Double.parseDouble(split[4]);
				attributes[2] = Double.parseDouble(split[5]);
				attributes[3] = Double.parseDouble(split[6]);

				if (Double.isNaN(attributes[0]) || Double.isNaN(attributes[1])
						|| Double.isNaN(attributes[3])
						|| Double.isNaN(attributes[2])) {
					throw new NaNException("Not a number exception");
				}

				DoubleArrayWritable attributeVector = new DoubleArrayWritable();
				attributeVector.setValueArray(attributes);

				SongDataPoint songDataPoint = new SongDataPoint(new Text(
						trackId), new Text(title), new Text(artistName),
						attributeVector);
				DoubleArrayWritable closestCentroid = centroids.get(0);
				double minDistance = attributeVector.distance(closestCentroid);
				for (int i = 1; i < centroids.size(); i++) {
					if (attributeVector.distance(centroids.get(i)) < minDistance) {
						closestCentroid = centroids.get(i);
						minDistance = attributeVector
								.distance(centroids.get(i));
					}
				}
				Text nearCentroid = new Text(closestCentroid.toString());
				context.write(nearCentroid, songDataPoint);
			} catch (NaNException e) {

			}
		}
	}

	public static class MapReduceKMeansReducer extends
			Reducer<Text, SongDataPoint, Text, NullWritable> {

		public void reduce(Text key, Iterable<SongDataPoint> values,
				Context context) throws IOException, InterruptedException {
			DoubleArrayWritable newCent = getNewCentroid(values);
			Text newCentroid = new Text(newCent.toString());
			context.write(newCentroid, NullWritable.get());

		}
	}

	public static class MapperClustering extends
			Mapper<Object, Text, Text, SongDataPoint> {

		ArrayList<DoubleArrayWritable> centroids;

		@Override
		protected void setup(
				Mapper<Object, Text, Text, SongDataPoint>.Context context)
				throws IOException, InterruptedException {
			centroids = new ArrayList<DoubleArrayWritable>();
			Configuration conf = context.getConfiguration();

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			ArrayList<Path> paths = new ArrayList<Path>();
			for (Path cacheFile : cacheFiles) {
				paths.add(cacheFile);
			}
			try {
				for (Path p : paths) {
					BufferedReader readBuffer1 = new BufferedReader(
							new FileReader(p.toString()));
					String line;
					while ((line = readBuffer1.readLine()) != null) {
						String[] split = line.split(",");
						double centroid[] = new double[4];
						centroid[0] = Double.parseDouble(split[0]);
						centroid[1] = Double.parseDouble(split[1]);
						centroid[2] = Double.parseDouble(split[2]);
						centroid[3] = Double.parseDouble(split[3]);
						DoubleArrayWritable centr = new DoubleArrayWritable();
						centr.setValueArray(centroid);
						centroids.add(centr);
					}
					readBuffer1.close();
				}
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, SongDataPoint>.Context context)
				throws IOException, InterruptedException {
			try {
				String str = value.toString();
				String split[] = str.split(",");

				String trackId = split[0];
				String title = split[1];
				String artistName = split[2];
				double[] attributes = new double[4];
				attributes[0] = Double.parseDouble(split[3]);
				attributes[1] = Double.parseDouble(split[4]);
				attributes[2] = Double.parseDouble(split[5]);
				attributes[3] = Double.parseDouble(split[6]);

				if (Double.isNaN(attributes[0]) || Double.isNaN(attributes[1])
						|| Double.isNaN(attributes[3])
						|| Double.isNaN(attributes[2])) {
					throw new NaNException("Not a number exception");
				}

				DoubleArrayWritable attributeVector = new DoubleArrayWritable();
				attributeVector.setValueArray(attributes);

				SongDataPoint songDataPoint = new SongDataPoint(new Text(
						trackId), new Text(title), new Text(artistName),
						attributeVector);
				DoubleArrayWritable closestCentroid = centroids.get(0);
				double minDistance = attributeVector.distance(closestCentroid);
				for (int i = 1; i < centroids.size(); i++) {
					if (attributeVector.distance(centroids.get(i)) < minDistance) {
						closestCentroid = centroids.get(i);
						minDistance = attributeVector
								.distance(centroids.get(i));
					}
				}
				Text nearCentroid = new Text(closestCentroid.toString());
				context.write(nearCentroid, songDataPoint);
			} catch (Exception e) {

			}
		}
	}

	public static class ReducerClustering extends
			Reducer<Text, SongDataPoint, Text, Text> {

		@Override
		protected void reduce(Text centroid,
				Iterable<SongDataPoint> songValues, Context context)
				throws IOException, InterruptedException {
			Text joinedData = joinDataPoint(songValues);
			context.write(centroid, joinedData);
		}

		private Text joinDataPoint(Iterable<SongDataPoint> values) {
			StringBuilder sb = new StringBuilder();

			for (SongDataPoint sdp : values) {
				String song = sdp.toString();
				sb.append(song).append("\t");
			}
			String finalCluster = sb.toString();
			return new Text(finalCluster);

		}
	}

	public static DoubleArrayWritable getNewCentroid(
			Iterable<SongDataPoint> values) {
		double totalArtistFamiliarity = 0;
		double totalArtistHottness = 0;
		double totalLoudness = 0;
		double totalTempo = 0;
		int count = 0;
		for (SongDataPoint sdp : values) {
			double[] attributes = sdp.getAttributesVector().getValueArray();
			totalArtistFamiliarity = totalArtistFamiliarity + attributes[0];
			totalArtistHottness = totalArtistHottness + attributes[1];
			totalLoudness = totalLoudness + attributes[2];
			totalTempo = totalTempo + attributes[3];
			count++;
		}
		double newCentroidArray[] = new double[4];
		newCentroidArray[0] = totalArtistFamiliarity / count;
		newCentroidArray[1] = totalArtistHottness / count;
		newCentroidArray[2] = totalLoudness / count;
		newCentroidArray[3] = totalTempo / count;
		DoubleArrayWritable newCentroid = new DoubleArrayWritable();
		newCentroid.setValueArray(newCentroidArray);
		return newCentroid;
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		long currentTime = System.currentTimeMillis();
		int count = Integer.parseInt(args[2]);
		final String centroidPath = "/centroid-";
		final String finalOutPath = "/finalOut";
		for (int i = 1; i <= count; i++) {
			Configuration confRand = new Configuration();

			String[] otherArgs = new GenericOptionsParser(confRand, args)
					.getRemainingArgs();
			if (otherArgs.length != 3) {
				System.err
						.println("Usage:<CsV Path> "
								+ "<Centroid File Path> <Final Out Path> <number of Clusters>");
				System.exit(2);
			}

			confRand.set("percentage", "0.7");
			confRand.set("limit", Integer.toString(i));
			Job job = new Job(confRand);
			job.setJobName("random Sampling");
			job.setJarByClass(MapReduceKMeans.class);
			job.setMapperClass(RandomSamplingMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			String path = otherArgs[1] + i + centroidPath + 0;
			FileOutputFormat.setOutputPath(job, new Path(path));
			while (!job.waitForCompletion(true)) {

			}

			int iteration = 0;

			Configuration conf = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			if (otherArgs.length != 3) {
				System.err
						.println("Usage:<CsV Path> "
								+ "<Centroid File Path> <Final Out Path> <number of Clusters>");
				System.exit(2);
			}
			Job job1 = new Job(conf);

			boolean flag = false;
			while (!flag) {
				URI uri = URI.create(otherArgs1[1] + i + centroidPath
						+ iteration);
				FileSystem fs = FileSystem.get(uri, conf);
				FileStatus[] items = fs.listStatus(new Path(otherArgs1[1] + i
						+ centroidPath + iteration));
				job1 = new Job(new Configuration());
				Configuration conf2 = job1.getConfiguration();
				job1.setJobName("Centroid Step");
				for (FileStatus f : items) {
					String p = f.getPath().toString();
					if (!p.contains("SUCCESS")) {
						DistributedCache.addCacheFile(new URI(p), conf2);
					}
				}
				iteration++;

				job1.setJarByClass(MapReduceKMeans.class);
				job1.setMapperClass(MapReduceKMeansMapper.class);
				job1.setReducerClass(MapReduceKMeansReducer.class);

				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(SongDataPoint.class);
				// job1.setNumReduceTasks(2);
				FileInputFormat.addInputPath(job1, new Path(otherArgs1[0]));
				FileOutputFormat.setOutputPath(job1, new Path(otherArgs1[1] + i
						+ centroidPath + iteration));

				job1.waitForCompletion(true);

				String oldPath = otherArgs1[1] + i + centroidPath
						+ (iteration - 1);
				// System.out.println(oldPath);
				String newPath = otherArgs1[1] + i + centroidPath + (iteration);
				// System.out.println(newPath);

				flag = compareHDFSFiles(oldPath, newPath, conf2, iteration);
			}
			Configuration confFinal = new Configuration();

			String[] otherArgs2 = new GenericOptionsParser(confRand, args)
					.getRemainingArgs();
			if (otherArgs.length != 3) {
				System.err.println("Usage:<CsV Path> "
						+ "<Centroid File Path> <Final Out Path> ");
				System.exit(2);
			}

			Job job2 = new Job(confFinal);
			job2.setJobName("Clustering Step");
			URI uri = URI.create(otherArgs2[1] + i + centroidPath + iteration);
			FileSystem fs = FileSystem.get(uri, conf);
			FileStatus[] items = fs.listStatus(new Path(otherArgs1[1] + i
					+ centroidPath + iteration));
			job2 = new Job(new Configuration());
			Configuration conf3 = job2.getConfiguration();
			for (FileStatus f : items) {
				String p = f.getPath().toString();
				if (!p.contains("SUCCESS")) {
					DistributedCache.addCacheFile(new URI(p), conf3);
				}
			}
			job2.setJarByClass(MapReduceKMeans.class);
			job2.setMapperClass(MapperClustering.class);
			job2.setReducerClass(ReducerClustering.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(SongDataPoint.class);
			FileInputFormat.addInputPath(job2, new Path(otherArgs2[0]));
			String finalPath = otherArgs1[1] + i + finalOutPath;
			FileOutputFormat.setOutputPath(job2, new Path(finalPath));
			job2.waitForCompletion(true);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Time taken to execute : "
				+ ((endTime - currentTime) / 1000));
	}

	private static boolean compareHDFSFiles(String oldPath, String newPath,
			Configuration conf2, int iteration) throws IOException,
			NumberFormatException, URISyntaxException {
		FileSystem fs = FileSystem.get(URI.create(oldPath), conf2);
		FileSystem fs1 = FileSystem.get(URI.create(newPath), conf2);
		FileStatus[] oldItems = fs.listStatus(new Path(oldPath));
		FileStatus[] newItems = fs1.listStatus(new Path(newPath));
		HashSet<ArrayList<Double>> oldFileItems = getItemsInSet(oldItems, fs);
		HashSet<ArrayList<Double>> newFileItems = getItemsInSet(newItems, fs1);
		System.out.println("Iteration " + iteration);
		System.out.println(oldFileItems);
		System.out.println(newFileItems);
		System.out.println(oldFileItems.size());
		System.out.println(newFileItems.size());
		System.out.println();
		System.out.println();
		if (oldFileItems.containsAll(newFileItems)
				&& newFileItems.containsAll(oldFileItems)) {
			return true;
		}
		return false;
	}

	private static HashSet<ArrayList<Double>> getItemsInSet(
			FileStatus[] oldItems, FileSystem fs) throws NumberFormatException,
			IOException, URISyntaxException {
		HashSet<ArrayList<Double>> items = new HashSet<ArrayList<Double>>();
		for (FileStatus f : oldItems) {
			String p = f.getPath().toString();
			// System.out.println(p);
			URI uri = new URI(p);
			Path path = new Path(uri.toString());
			FSDataInputStream fsin = fs.open(path);
			DataInputStream in = new DataInputStream(fsin);
			if (!p.contains("SUCCESS")) {
				BufferedReader readBuffer1 = new BufferedReader(
						new InputStreamReader(in));
				String line;
				while ((line = readBuffer1.readLine()) != null) {
					String[] split = line.split(",");
					ArrayList<Double> centroid = new ArrayList<Double>();
					centroid.add(Double.parseDouble(split[0]));
					centroid.add(Double.parseDouble(split[1]));
					centroid.add(Double.parseDouble(split[2]));
					centroid.add(Double.parseDouble(split[3]));
					items.add(centroid);
				}
				readBuffer1.close();
			}
			fsin.close();
			in.close();
		}
		return items;
	}

}
