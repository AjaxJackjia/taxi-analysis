package com.gis.model;

import java.awt.Polygon;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import com.taxi.api.GISCalculationService;

public class Trip {
	private List<Long> vertices;
	private List<Map.Entry<Double, Double>> traces;
	private double length;
	private long id;

	public Trip(long id) {
		this.id = id;
		vertices = new ArrayList<Long>();
		traces = new LinkedList<Map.Entry<Double, Double>>();
	}

	public List<Long> getVertices()
	{
		return vertices;
	}

	public void setVertices(List<Long> points)
	{
		this.vertices = points;
	}

	public long getId()
	{
		return id;
	}

	public void setId(long id)
	{
		this.id = id;
	}

	public boolean add(Long e)
	{
		return vertices.add(e);
	}

	@Override
	public String toString()
	{
		return "\n" + id + "\t" + vertices.toString();
	}

	public List<Map.Entry<Double, Double>> getTraces()
	{
		return traces;
	}

	public double getLength()
	{
		return length;
	}

	public void setLength(double length)
	{
		this.length = length;
	}

	public boolean add(Entry<Double, Double> e)
	{
		if (traces.size() == 0)
			return traces.add(e);
		if (traces.get(traces.size() - 1).equals(e))
			return false;
		traces.add(e);
		return true;
	}

	public double similarityBetween(Trip trip, int extend_orthogonal)
	{
//		System.out.println("Similarity:" + id + " " + trip.id);
		int cnt = 0;
		for (Map.Entry<Double, Double> point : trip.traces) {
			if (isOnTrip(point.getKey(), point.getValue(),
					extend_orthogonal * 3 / 2, 0))
				++cnt;
		}
		return 1. * cnt / trip.traces.size();
	}

	private boolean isOnTrip(double lng, double lat, int extend_orthogonal,
			int extend_parallel, int from_idx)
	{
		// Unit vector between adjacent point
		double v_x, v_y;
		// Orthogonal vector of unit vector above
		double n_x, n_y;
		double midpoint_x0, midpoint_y0;
		double midpoint_x1, midpoint_y1;
		int x, y;
		int i = 0;
		Iterator<Map.Entry<Double, Double>> it = traces.iterator();
		Map.Entry<Double, Double> pre_point = null;
		while (i <= from_idx && it.hasNext()) {
			pre_point = it.next();
			++i;
		}
		while (it.hasNext()) {
			Map.Entry<Double, Double> cur_point = it.next();
			if (cur_point.equals(pre_point))
				continue;
			v_x = cur_point.getKey() - pre_point.getKey();
			v_y = cur_point.getValue() - pre_point.getValue();
			double tmp = Math.sqrt(v_x * v_x + v_y * v_y);
			v_x /= tmp;
			v_y /= tmp;

			n_x = v_y;
			n_y = -v_x;
			Polygon polygon = new Polygon();
			midpoint_x0 = pre_point.getKey() - 0.0000101 * extend_parallel
					* v_x;
			midpoint_y0 = pre_point.getValue() - 0.0000101 * extend_parallel
					* v_y;
			midpoint_x1 = cur_point.getKey() + 0.0000101 * extend_parallel
					* v_x;
			midpoint_y1 = cur_point.getValue() + 0.0000101 * extend_parallel
					* v_y;
			pre_point = cur_point;

			x = (int) (midpoint_x0 * 1000000);
			y = (int) (midpoint_y0 * 1000000);
			polygon.addPoint((int) (x + n_x * 10 * extend_orthogonal),
					(int) (y + n_y * 10 * extend_orthogonal));
			polygon.addPoint((int) (x - n_x * 10 * extend_orthogonal),
					(int) (y - n_y * 10 * extend_orthogonal));
			x = (int) (midpoint_x1 * 1000000);
			y = (int) (midpoint_y1 * 1000000);
			polygon.addPoint((int) (x - n_x * 10 * extend_orthogonal),
					(int) (y - n_y * 10 * extend_orthogonal));
			polygon.addPoint((int) (x + n_x * 10 * extend_orthogonal),
					(int) (y + n_y * 10 * extend_orthogonal));
			x = (int) (lng * 1000000);
			y = (int) (lat * 1000000);
			if (polygon.contains(x, y)) {
				return true;
			}
		}

		return false;
	}

	public boolean mergeWith(Trip trip)
	{
		int size_before_merge = traces.size();
		ListIterator<Map.Entry<Double, Double>> it = trip.traces.listIterator();
		Map.Entry<Double, Double> first = it.next();
		Map.Entry<Double, Double> second = it.next();
		Map.Entry<Double, Double> third = it.next();
		while (it.hasNext()) {
			double x1 = first.getKey();
			double y1 = first.getValue();
			double x2 = second.getKey();
			double y2 = second.getValue();
			double x3 = third.getKey();
			double y3 = third.getValue();

			double v1_x = x2 - x1;
			double v1_y = y2 - y1;
			double v2_x = x3 - x2;
			double v2_y = y3 - y2;
			// Dramastic change in direction(more than Pi/4
			Map.Entry<Double, Double> predict;
			if ((v1_x * v2_x + v1_y * v2_y)
					/ Math.sqrt((v1_x * v1_x + v1_y * v1_y)
							* (v2_x * v2_x + v2_y * v2_y)) < 0.707
					|| GISCalculationService.st_distance_sphere(x2, y2, x3, y3) > 250) {
				predict = predictNextTrace(x2, y2,
						v2_x / Math.sqrt((v2_x * v2_x + v2_y * v2_y)), v2_y
								/ Math.sqrt((v2_x * v2_x + v2_y * v2_y)), trip);
				if (predict != null) {
					it.previous();
					it.add(predict);
					it.next();
				}
			}
			first = second;
			second = third;
			third = it.next();
		}
		System.out.println(size_before_merge + " " + traces.size());
		if (traces.size() > size_before_merge)
			return true;
		return false;
	}

	private boolean isOnTrip(double lng, double lat, int extend_orthogonal,
			int from_idx)
	{
		return isOnTrip(lng, lat, extend_orthogonal, 20, from_idx);
	}

	/**
	 * 
	 * @param lng
	 * @param lat
	 * @param u_x
	 *            x coordinate of unit vector
	 * @param u_y
	 *            y coordinate of unit vector
	 * @param trip
	 * @return
	 */
	private Map.Entry<Double, Double> predictNextTrace(double lng, double lat,
			double u_x, double u_y, Trip trip)
	{
		for (Map.Entry<Double, Double> point : trip.traces) {
			double v_x = point.getKey() - lng;
			double v_y = point.getValue() - lat;
			// Nearby(250) and less that Pi/6
			if ((u_x * v_x + u_y * v_y) / Math.sqrt(v_x * v_x + v_y * v_y) >= 0.866
					&& GISCalculationService.st_distance_sphere(lng, lat,
							point.getKey(), point.getValue()) <= 200
					&& GISCalculationService.st_distance_sphere(lng, lat,
							point.getKey(), point.getValue()) >= 50)
				return point;
		}
		return null;
	}
}
