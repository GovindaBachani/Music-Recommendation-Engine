package com.kmeans.trial;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class Kmeans {

	public static int noOfClusters = 70;
	public static HashSet<Double> randomNumber = new HashSet<Double>();

	static HashMap<String, ArrayList<Double>> dataPoints = new HashMap<String, ArrayList<Double>>();

	static HashMap<String, ArrayList<String>> songData = new HashMap<String, ArrayList<String>>();

	static void populateSongData(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String s;
		s = reader.readLine();
		while ((s = reader.readLine()) != null) {
			try {
				String[] split = s.split(",");
				String trackId = split[0];
				String trackName = split[1];
				String artistName = split[2];
				ArrayList<String> trackData = new ArrayList<String>();
				trackData.add(trackName);
				trackData.add(artistName);
				Double artistFamilarity = Double.valueOf(split[3]);
				if (Double.isNaN(artistFamilarity)) {
					continue;
				}
				songData.put(trackId, trackData);
			} catch (Exception e) {

			}
		}
		reader.close();
	}

	static HashMap<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> clusters;

	public static ArrayList<ArrayList<Double>> getCentroids(int noOfClusters) {
		ArrayList<ArrayList<Double>> centroids = new ArrayList<ArrayList<Double>>();
		for (int i = 0; i < noOfClusters; i++) {
			ArrayList<Double> centroid = new ArrayList<Double>();

			double random = getUniqueRandom();

			double artistFamilarity = random;
			double artistHotness = 1.08250255672612 * random;
			double loudness = 51 * random;
			loudness = loudness * -1;
			double tempo = 262 * random;
			centroid.add(artistFamilarity);
			centroid.add(artistHotness);
			centroid.add(loudness);
			centroid.add(tempo);
			centroids.add(centroid);
			randomNumber.add(random);

		}
		return centroids;

	}

	private static double getUniqueRandom() {
		Random r = new Random();
		double random = r.nextDouble();
		if (randomNumber.contains(random)) {
			while (!randomNumber.contains(random)) {
				random = r.nextDouble();
			}
		}

		return random;
	}

	public static ArrayList<Double> findMinDistanceCluster(
			ArrayList<ArrayList<Double>> centroids, ArrayList<Double> attributes) {
		double minDistance = Double.MAX_VALUE;
		ArrayList<Double> assignedCentroid = new ArrayList<Double>();
		for (ArrayList<Double> centroid : centroids) {
			double distance;
			double square = 0;
			for (int i = 0; i < 4; i++) {
				square = square + (centroid.get(i) - attributes.get(i))
						* (centroid.get(i) - attributes.get(i));
			}
			distance = Math.sqrt(square);
			if (distance < minDistance) {
				minDistance = distance;
				assignedCentroid = centroid;
			}
		}
		return assignedCentroid;
	}

	public static void assignCluster(ArrayList<ArrayList<Double>> centroids) {
		clusters = new HashMap<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>>();
		for (Entry<String, ArrayList<Double>> e : dataPoints.entrySet()) {
			ArrayList<Double> centroid = findMinDistanceCluster(centroids,
					e.getValue());
			if (clusters.containsKey(centroid)) {
				ArrayList<Entry<String, ArrayList<Double>>> currentGroup = clusters
						.get(centroid);
				currentGroup.add(e);
				clusters.put(centroid, currentGroup);
			} else {
				ArrayList<Entry<String, ArrayList<Double>>> newGroup = new ArrayList<Entry<String, ArrayList<Double>>>();
				newGroup.add(e);
				clusters.put(centroid, newGroup);
			}
		}
	}

	public static void setDataPoints(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String s;
		s = reader.readLine();
		while ((s = reader.readLine()) != null) {
			try {
				String[] split = s.split(",");
				String trackId = split[0];
				Double artistFamilarity = Double.valueOf(split[3]);
				if (Double.isNaN(artistFamilarity)) {
					continue;
				}
				Double artistHotness = Double.valueOf(split[4]);
				Double loudness = Double.valueOf(split[5]);
				Double tempo = Double.valueOf(split[6]);
				ArrayList<Double> value = new ArrayList<Double>();
				value.add(artistFamilarity);
				value.add(artistHotness);
				value.add(loudness);
				value.add(tempo);
				dataPoints.put(trackId, value);
			} catch (Exception e) {

			}
		}
		reader.close();
	}

	public static void main(String[] args) throws IOException {
		setDataPoints(new File("MillionSongDataSet.csv"));
		populateSongData(new File("MillionSongDataSet.csv"));
		ArrayList<ArrayList<Double>> centroids = getCentroids(noOfClusters);
		assignCluster(centroids);
		iterativeAssignCluster(centroids);
		ArrayList<String> songDetails = new ArrayList<String>();
		songDetails.add("Baat Ban Jaye");
		songDetails.add("Biddu");
		String trackId = findTrackId(new File("MillionSongDataSet.csv"),
				songDetails);
		suggestTopTenSong(trackId);
	}

	private static String findTrackId(File file, ArrayList<String> songDetails)
			throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String s;
		s = reader.readLine();
		while ((s = reader.readLine()) != null) {
			try {
				String[] split = s.split(",");
				String trackId = split[0];
				String trackName = split[1];
				String artistName = split[2];
				if (trackName.equals(songDetails.get(0))
						&& artistName.equals(songDetails.get(1))) {
					return trackId;
				}
			} catch (Exception e) {

			}
		}
		reader.close();
		return null;
	}

	private static void suggestTopTenSong(String trackId) {

		ArrayList<Double> songAttributes = dataPoints.get(trackId);

		Entry<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> nearestCluster = getNearestCluster(songAttributes);

		ArrayList<ArrayList<String>> topSongs = getTopTenSongsFromCluster(
				nearestCluster, songAttributes);
		System.out.println("Top Ten Similar Songs");
		int count = 1;
		for(ArrayList<String> topSong : topSongs){
			System.out.println(count +". Song Name:"+topSong.get(0) + " \nArtist Name:"+topSong.get(1));
			count++;
		}

	}

	@SuppressWarnings("unchecked")
	private static ArrayList<ArrayList<String>> getTopTenSongsFromCluster(
			Entry<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> nearestCluster, ArrayList<Double> songAttributes) {
		ArrayList<Entry<String, ArrayList<Double>>> songs = nearestCluster.getValue();
		HashMap<String, Double> songDistance = new HashMap<String, Double>();
		for(Entry<String, ArrayList<Double>> song : songs){
			double distance = getSongDistance(song.getValue(), songAttributes);
			songDistance.put(song.getKey(), distance);
		}
		
		songDistance = (HashMap<String, Double>) sortByComparator(songDistance);
		int count = 1;
		ArrayList<ArrayList<String>> topSongs = new ArrayList<ArrayList<String>>();
		for(Entry<String,Double> songD : songDistance.entrySet()){
			ArrayList<String> songNameAndTitle = songData.get(songD.getKey());
			topSongs.add(songNameAndTitle);
			count++;
			if(count==11){
				break;
			}
		}
		
		return topSongs;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Map sortByComparator(Map unsortMap) {

		List list = new LinkedList(unsortMap.entrySet());
		// sort list based on comparator
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o2)).getValue())
						.compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		// put sorted list into map again
		// LinkedHashMap make sure order in which keys were inserted
		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	private static double getSongDistance(ArrayList<Double> value,
			ArrayList<Double> songAttributes) {
		double distance;
		double square = 0;
		for (int i = 0; i < 4; i++) {
			square = square + (value.get(i) - songAttributes.get(i))
					* (value.get(i) - songAttributes.get(i));
		}
		distance = Math.sqrt(square);
		return distance;
	}

	private static Entry<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> getNearestCluster(
			ArrayList<Double> songAttributes) {
		Entry<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> nearestCluster = null;
		double minDistance = Double.MAX_VALUE;
		for (Entry<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> cluster : clusters
				.entrySet()) {
			ArrayList<Double> clusterAttribute = cluster.getKey();
			double distance;
			double square = 0;
			for (int i = 0; i < 4; i++) {
				square = square
						+ (clusterAttribute.get(i) - songAttributes.get(i))
						* (clusterAttribute.get(i) - songAttributes.get(i));
			}
			distance = Math.sqrt(square);
			if (distance < minDistance) {
				minDistance = distance;
				nearestCluster = cluster;
			}
		}
		return nearestCluster;
	}

	private static void iterativeAssignCluster(
			ArrayList<ArrayList<Double>> oldCentroids) {
		ArrayList<ArrayList<Double>> newCentroids = assignNewCentroids(clusters);
		while (!oldCentroids.equals(newCentroids)) {
			assignCluster(newCentroids);
			oldCentroids = newCentroids;
			newCentroids = assignNewCentroids(clusters);
		}
	}

	private static ArrayList<ArrayList<Double>> assignNewCentroids(
			HashMap<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> clusters) {
		ArrayList<ArrayList<Double>> newCentroids = new ArrayList<ArrayList<Double>>();
		for (Entry<ArrayList<Double>, ArrayList<Entry<String, ArrayList<Double>>>> e : clusters
				.entrySet()) {
			double a = e.getValue().get(0).getValue().get(0);
			if (Double.isNaN(a)) {
				continue;
			}
			ArrayList<Entry<String, ArrayList<Double>>> newClusterData = e
					.getValue();
			double newArtistFamiliarity = 0;
			double newArtistHotness = 0;
			double newLoudness = 0;
			double newTempo = 0;
			for (Entry<String, ArrayList<Double>> entry : newClusterData) {
				newArtistFamiliarity = newArtistFamiliarity
						+ entry.getValue().get(0);
				newArtistHotness = newArtistHotness + entry.getValue().get(1);
				newLoudness = newLoudness + entry.getValue().get(2);
				newTempo = newTempo + entry.getValue().get(3);
			}
			newArtistFamiliarity = newArtistFamiliarity / e.getValue().size();
			newArtistHotness = newArtistHotness / e.getValue().size();
			newLoudness = newLoudness / e.getValue().size();
			newTempo = newTempo / e.getValue().size();
			ArrayList<Double> centroid = new ArrayList<Double>();
			centroid.add(newArtistFamiliarity);
			centroid.add(newArtistHotness);
			centroid.add(newLoudness);
			centroid.add(newTempo);
			newCentroids.add(centroid);
		}
		return newCentroids;
	}
}
