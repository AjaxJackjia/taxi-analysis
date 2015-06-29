package com.taxi.api;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.Point;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.db.DBUtil;

@Path("/calculation")
public class GISCalculationService extends GISBaseService {
	static PreparedStatement stmt_distance_sphere;
	static {
		try {
			stmt_distance_sphere = DBUtil
					.getInstance()
					.createSqlStatement(
							"select st_distance_sphere(st_point(?,?), "
									+ "st_point(?,?))");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@GET
	@Path("/GetAreaSections")
	public Response getAreaSections(@QueryParam("north") String p_north,
			@QueryParam("south") String p_south,
			@QueryParam("west") String p_west, @QueryParam("east") String p_east)
			throws Exception
	{
		JSONObject res = new JSONObject();
		FeatureCollection feature_collection = new FeatureCollection();

		double north = Double.parseDouble(p_north);
		double west = Double.parseDouble(p_west);
		double south = Double.parseDouble(p_south);
		double east = Double.parseDouble(p_east);

		String sql = "select "
				+ "	T1.id as section_id, "
				+ "	T2.id as from_node, st_x(T2.geom) as from_lng, st_y(T2.geom) as from_lat,  "
				+ "	T3.id as to_node, st_x(T3.geom) as to_lng, st_y(T3.geom) as to_lat "
				+ "from " + "	taxi.sections T1, nodes T2, nodes T3 " + "where "
				+ "	T1.from_node = T2.id and " + "	T1.to_node = T3.id and "
				+ "	st_x(T2.geom) >= ? and " + "	st_x(T2.geom) <= ? and "
				+ "	st_y(T2.geom) >= ? and " + "	st_y(T2.geom) <= ? and "
				+ "	st_x(T3.geom) >= ? and " + "	st_x(T3.geom) <= ? and "
				+ "	st_y(T3.geom) >= ? and " + "	st_y(T3.geom) <= ?";
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sql, west, east, south,
				north, west, east, south, north);
		ResultSet rs_stmt = stmt.executeQuery();
		JSONObject p = null;
		while (rs_stmt.next()) {
			Feature feature = new Feature();
			LineString line_string = new LineString();
			line_string.add(new LngLatAlt(rs_stmt.getDouble("from_lng"),
					rs_stmt.getDouble("from_lat")));
			line_string.add(new LngLatAlt(rs_stmt.getDouble("to_lng"), rs_stmt
					.getDouble("to_lat")));
			feature.setProperty("section_id", rs_stmt.getString("section_id"));
			feature.setProperty("from_node", rs_stmt.getString("from_node"));
			feature.setProperty("from_lng", rs_stmt.getDouble("from_lng"));
			feature.setProperty("from_lat", rs_stmt.getDouble("from_lat"));
			feature.setProperty("to_node", rs_stmt.getString("to_node"));
			feature.setProperty("to_lng", rs_stmt.getDouble("to_lng"));
			feature.setProperty("to_lat", rs_stmt.getDouble("to_lat"));

			feature.setGeometry(line_string);
			feature_collection.add(feature);
		}
		rs_stmt.close();
		closeStatementResource(stmt);

		res.put("sections",
				new ObjectMapper().writeValueAsString(feature_collection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("/GetCalculationResult")
	public Response getCalculationResult(@QueryParam("start") String p_start,
			@QueryParam("end") String p_end, @QueryParam("gps") String p_gps,
			@QueryParam("atr_cur") String p_atr_cur,
			@QueryParam("rec") String p_rec,
			@QueryParam("atr_bench") String p_atr_bench) throws Exception
	{
		JSONObject res = new JSONObject();
		FeatureCollection gps_feature_collection = new FeatureCollection();
		FeatureCollection section_feature_collection = new FeatureCollection();

		// step 1: convert string to value
		p_start = p_start.replaceAll("[\" {}\\n\\t]", "");
		p_end = p_end.replaceAll("[\" {}\\n\\t]", "");
		p_gps = p_gps.replaceAll("[\" {}\\n\\t]", "");
		p_atr_cur = p_atr_cur.replaceAll("[\" {}\\n\\t]", "");
		p_rec = p_rec.replaceAll("[\" {}\\n\\t]", "");
		p_atr_bench = p_atr_bench.replaceAll("[\" {}\\n\\t]", "");

		double[] start_double = new double[2];
		double[] end_double = new double[2];
		ArrayList<double[]> gpsPoints = new ArrayList<double[]>();
		ArrayList<long[]> rec = new ArrayList<long[]>();
		long[] atr_cur, atr_bench;

		// start, end
		String[] tmp_start = p_start.split(",");
		start_double[0] = Double.parseDouble(tmp_start[1]);
		start_double[1] = Double.parseDouble(tmp_start[0]);
		String[] tmp_end = p_end.split(",");
		end_double[0] = Double.parseDouble(tmp_end[1]);
		end_double[1] = Double.parseDouble(tmp_end[0]);

		// gps
		String[] tmp_gps = p_gps.split("#");
		for (int i = 0; i < tmp_gps.length; i++) {
			String[] p = tmp_gps[i].split(",");
			double[] pd = new double[2];
			pd[0] = Double.parseDouble(p[1]);
			pd[1] = Double.parseDouble(p[0]);
			gpsPoints.add(pd);
		}
		// atr_cur
		atr_cur = getAtr(p_atr_cur);
		// atr_bench
		atr_bench = getAtr(p_atr_bench);
		// Rec
		String[] tmp_rec = p_rec.split("#");
		for (int i = 0; i < tmp_rec.length; i++) {
			rec.add(getAtr(tmp_rec[i]));
		}

		// step 1.5: return GeoJSON for front-end
		// GPS points GeoJSON
		for (int i = 0; i < gpsPoints.size(); ++i) {
			Feature feature = new Feature();
			Point point = new Point(gpsPoints.get(i)[0], gpsPoints.get(i)[1]);
			feature.setGeometry(point);
			feature.setProperty("lng", point.getCoordinates().getLongitude());
			feature.setProperty("lat", point.getCoordinates().getLatitude());

			gps_feature_collection.add(feature);
		}
		// section GeoJSON
		// atr_cur
		Feature[] atr_cur_features = getSectionFeatureCollection(atr_cur,
				"atr_cur");
		for (int i = 0; i < atr_cur_features.length; i++) {
			section_feature_collection.add(atr_cur_features[i]);
		}
		// atr_bench
		Feature[] atr_bench_features = getSectionFeatureCollection(atr_bench,
				"atr_bench");
		for (int i = 0; i < atr_bench_features.length; i++) {
			section_feature_collection.add(atr_bench_features[i]);
		}
		// rec sections
		for (int i = 0; i < rec.size(); i++) {
			long[] atr = rec.get(i);
			Feature[] atr_rec_features = getSectionFeatureCollection(atr, "rec");
			for (int j = 0; j < atr_rec_features.length; j++) {
				section_feature_collection.add(atr_rec_features[j]);
			}
		}

		// step 2: convert atr to vector according to start and end point
		ArrayList<double[]> atr_cur_vectors = generateVectorFromAtr(
				start_double, end_double, atr_cur);
		for (int i = 0; i < atr_cur_vectors.size(); i++) {
			double[] t = atr_cur_vectors.get(i);
			System.out.println("Point(" + t[0] + " " + t[1] + ")"
					+ " -> Point(" + t[2] + "," + t[3] + ")        "
					+ "LINESTRING (" + t[0] + " " + t[1] + "," + t[2] + " "
					+ t[3] + ")");
		}
		ArrayList<double[]> atr_bench_vectors = generateVectorFromAtr(
				start_double, end_double, atr_bench);
		ArrayList<ArrayList<double[]>> rec_vectors = new ArrayList<ArrayList<double[]>>();
		for (int i = 0; i < rec.size(); i++) {
			rec_vectors.add(generateVectorFromAtr(start_double, end_double,
					rec.get(i)));
		}

		// step 3: calculate score_dir
		System.out.println("direction score:");
		JSONArray score_dir_array = new JSONArray();
		for (int i = 0; i < atr_cur_vectors.size(); i++) {
			double score_dir = calculateScoreDir(atr_cur_vectors.get(i),
					atr_bench_vectors, rec_vectors);
			score_dir_array.put(score_dir);
			System.out.println(i + ":" + score_dir);
		}

		// return result
		res.put("gps",
				new ObjectMapper().writeValueAsString(gps_feature_collection));
		res.put("sections", new ObjectMapper()
				.writeValueAsString(section_feature_collection));
		JSONObject figureData = new JSONObject();
		figureData.put("direction", score_dir_array);
		figureData.put("distance", "");
		figureData.put("fusion", "");
		res.put("figure", figureData);

		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	// get section GeoJSON FeatureCollection
	public Feature[] getSectionFeatureCollection(long[] p_atr, String p_atr_type)
			throws Exception
	{
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement("select taxi.get_section(?)");
		stmt.getConnection().setAutoCommit(false);
		ResultSet rs = null;
		Feature[] f = new Feature[p_atr.length];
		for (int i = 0; i < p_atr.length; i++) {
			Feature feature = new Feature();
			LineString line_string = new LineString();
			stmt.setLong(1, p_atr[i]);
			rs = stmt.executeQuery();
			if (rs.next())
				rs = (ResultSet) rs.getObject(1);
			while (rs.next()) {
				line_string.add(new LngLatAlt(rs.getDouble("from_lng"), rs
						.getDouble("from_lat")));
				line_string.add(new LngLatAlt(rs.getDouble("to_lng"), rs
						.getDouble("to_lat")));
				feature.setProperty("way_id", rs.getLong("way_id"));
				feature.setProperty("way_name", rs.getString("way_name"));
				feature.setProperty("type", p_atr_type);
				feature.setProperty("seq", i + 1);
				feature.setProperty("section_id", p_atr[i]);
			}
			feature.setGeometry(line_string);
			f[i] = feature;
		}
		closeResultSetResource(rs);
		closeStatementResource(stmt);
		return f;
	}

	// calculate direction score
	public double calculateScoreDir(double[] p_sec_cur_vector,
			ArrayList<double[]> p_bench_vector,
			ArrayList<ArrayList<double[]>> p_rec_vectors)
	{
		double sita = 0.1;
		double kexi = 0.63272;
		double simBench = 0, lengthBench = 0;
		double simCdd = 0, lengthCdd = 0;
		// flag
		boolean benchHasCur = false;

		// cur vector
		double[] x_from = { p_sec_cur_vector[0], p_sec_cur_vector[1] };
		double[] x_to = { p_sec_cur_vector[2], p_sec_cur_vector[3] };

		/* sim bench */
		// flag
		if (hasVector(p_bench_vector, p_sec_cur_vector)) {
			benchHasCur = true;
		}

		for (int i = 0; i < p_bench_vector.size(); i++) {
			double[] p = p_bench_vector.get(i);
			// bench vector
			double[] y_from = { p[0], p[1] };
			double[] y_to = { p[2], p[3] };

			// support distance
			// simBench += calculateGPSDistance(y_from, y_to) *
			// calculateSimilarity(x_from, x_to, y_from, y_to);
			// lengthBench += calculateGPSDistance(y_from, y_to);

			// number
			if (calculateSimilarity(x_from, x_to, y_from, y_to) > sita) {
				simBench++;
			}
			lengthBench++;
		}
		/* sim cdd */
		for (int i = 0; i < p_rec_vectors.size(); i++) {
			ArrayList<double[]> atr = p_rec_vectors.get(i);

			for (int j = 0; j < atr.size(); j++) {
				double[] p = atr.get(j);
				// cdd vector
				double[] y_from = { p[0], p[1] };
				double[] y_to = { p[2], p[3] };

				// support distance
				// simCdd += calculateGPSDistance(y_from, y_to) *
				// calculateSimilarity(x_from, x_to, y_from, y_to);
				// lengthCdd += calculateGPSDistance(y_from, y_to);

				// number
				if (calculateSimilarity(x_from, x_to, y_from, y_to) > sita) {
					simCdd++;
				}
				lengthCdd++;
			}
		}
		System.out.println("simBench:" + simBench);
		System.out.println("lengthBench:" + lengthBench);
		System.out.println("simCdd:" + simCdd);
		System.out.println("lengthCdd:" + lengthCdd);

		if (benchHasCur) {
			return 0;
		}

		return 1 - (kexi * simBench / lengthBench + (1 - kexi) * simCdd
				/ lengthCdd);
	}

	// generate section vector from long[]
	// return ArrayList<double[]> which stands for a series of vector from start
	// to end and double[] has four columns, the first two col is from
	// point<lng,lat>, and last two col is to point<lng,lat>
	// test: [77241, 77242, 77243, 227887, 227886, 159759, 207663] from:
	// -122.433572,37.769178 , to:-122.429926,37.766063
	public ArrayList<double[]> generateVectorFromAtr(double[] p_start,
			double[] p_end, long[] p_atr) throws Exception
	{
		ArrayList<double[]> vectors = new ArrayList<double[]>();

		// //////////////////////////////////////////
		// step 1: find atr start node and end node
		// //////////////////////////////////////////
		Long startNodeId = 1L;
		HashMap<Long, long[]> section_nodes = new HashMap<Long, long[]>();// <sectionId,
																			// [nodeId1,
																			// nodeId2]>
		HashMap<Long, Integer> numberMap = new HashMap<Long, Integer>();// <nodeId,
																		// number>
		HashMap<Long, double[]> points = new HashMap<Long, double[]>();// <nodeId,
																		// [lng,
																		// lat]>
		String sql = "select "
				+ "	T1.id as section_id, "
				+ "	T2.id as from_node, st_x(T2.geom) as from_lng, st_y(T2.geom) as from_lat,  "
				+ "	T3.id as to_node, st_x(T3.geom) as to_lng, st_y(T3.geom) as to_lat "
				+ "from " + "	taxi.sections T1, nodes T2, nodes T3 " + "where "
				+ "	T1.from_node = T2.id and " + "	T1.to_node = T3.id and "
				+ "	T1.id = ?";
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sql);
		ResultSet rs_stmt = null;
		for (int i = 0; i < p_atr.length; i++) {
			stmt.setObject(1, p_atr[i]);
			rs_stmt = stmt.executeQuery();
			while (rs_stmt.next()) {
				Long from_node = rs_stmt.getLong("from_node");
				Long to_node = rs_stmt.getLong("to_node");
				double[] p_from = { rs_stmt.getDouble("from_lng"),
						rs_stmt.getDouble("from_lat") };
				double[] p_to = { rs_stmt.getDouble("to_lng"),
						rs_stmt.getDouble("to_lat") };
				// add to points
				points.put(from_node, p_from);
				points.put(to_node, p_to);
				// add to node_section
				long[] nodeIds = { from_node, to_node };
				section_nodes.put(p_atr[i], nodeIds);
				// add number
				if (numberMap.containsKey(from_node)) {
					numberMap.put(from_node, numberMap.get(from_node) + 1);
				} else {
					numberMap.put(from_node, 1);
				}
				if (numberMap.containsKey(to_node)) {
					numberMap.put(to_node, numberMap.get(to_node) + 1);
				} else {
					numberMap.put(to_node, 1);
				}
			}
			rs_stmt.close();
		}
		closeStatementResource(stmt);

		// check hashmap, find node id which number equals one and verify atr
		// whether its element is not adjacent
		ArrayList<Long> startOrEnd = new ArrayList<Long>();
		Iterator iter = numberMap.entrySet().iterator();
		int verify_number = 0;
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Long key = (Long) entry.getKey();
			Integer val = (Integer) entry.getValue();
			if (val.intValue() == 1) {
				startOrEnd.add(key);
				verify_number++;
			}
		}
		if (verify_number != 2) {
			System.out.println("Count number:" + verify_number);
			System.err
					.println("Atr is not correct!!(not adjacent or repeat section)");
			throw new Exception();
		}

		// compare distance with start point
		if (calculateDistance(p_start, points.get(startOrEnd.get(0))) <= calculateDistance(
				p_start, points.get(startOrEnd.get(1)))) {
			startNodeId = startOrEnd.get(0);
		} else {
			startNodeId = startOrEnd.get(1);
		}

		System.out.println("Start:   " + startNodeId);
		// //////////////////////////////////////////////////////
		// step 2: sort points according to start and end point
		// //////////////////////////////////////////////////////
		// flag
		iter = numberMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Long key = (Long) entry.getKey();
			numberMap.put(key, 0);
		}
		// ////////////////////////////
		for (int i = 0; i < p_atr.length; i++) {
			System.out.println("nodes " + i + " : " + startNodeId);
			double[] vector = new double[4];
			double[] first = points.get(startNodeId);
			vector[0] = first[0];
			vector[1] = first[1];
			numberMap.put(startNodeId, 1);

			iter = section_nodes.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				Long a = (Long) entry.getKey();
				long[] b = (long[]) entry.getValue();
				if (numberMap.get(b[0]) == 1 && numberMap.get(b[1]) == 0) {
					double[] second = points.get(b[1]);
					vector[2] = second[0];
					vector[3] = second[1];
					startNodeId = b[1];// next point
					numberMap.put(startNodeId, 1);
					break;
				} else if (numberMap.get(b[0]) == 0 && numberMap.get(b[1]) == 1) {
					double[] second = points.get(b[0]);
					vector[2] = second[0];
					vector[3] = second[1];
					startNodeId = b[0];// next point
					numberMap.put(startNodeId, 1);
					break;
				}
			}
			// add to vectors
			vectors.add(vector);
		}
		System.out.println("nodes " + p_atr.length + " : " + startNodeId);

		return vectors;
	}

	// get atr from string
	public long[] getAtr(String atr_str)
	{
		String[] ids = atr_str.split(",");
		long[] atr = new long[ids.length];
		for (int i = 0; i < atr.length; i++) {
			atr[i] = Long.parseLong(ids[i]);
		}
		return atr;
	}

	// double[] : 0 -> lng , 1 --> lat
	public double calculateSimilarity(double[] x_from, double[] x_to,
			double[] y_from, double[] y_to)
	{
		// vector x
		double x0 = (x_to[0] - x_from[0]);
		double x1 = (x_to[1] - x_from[1]);
		// vector y
		double y0 = (y_to[0] - y_from[0]);
		double y1 = (y_to[1] - y_from[1]);

		double _t = (x0 * y0 + x1 * y1)
				/ (Math.sqrt(x0 * x0 + x1 * x1) * Math.sqrt(y0 * y0 + y1 * y1));
		return _t;
	}

	// double[] : 0 -> lng , 1 --> lat
	public double calculateDistance(double[] p_from, double[] p_to)
	{
		return Math.sqrt((p_to[0] - p_from[0]) * (p_to[0] - p_from[0])
				+ (p_to[1] - p_from[1]) * (p_to[1] - p_from[1]));
	}

	// return actual distance between two gps points
	public double calculateGPSDistance(double[] p_from, double[] p_to)
	{
		double _eQuatorialEarthRadius = 6378.1370D;
		double _d2r = (Math.PI / 180D);

		double dlong = (p_to[0] - p_from[0]) * _d2r;
		double dlat = (p_to[1] - p_from[1]) * _d2r;
		double a = Math.pow(Math.sin(dlat / 2D), 2D)
				+ Math.cos(p_from[1] * _d2r) * Math.cos(p_to[1] * _d2r)
				* Math.pow(Math.sin(dlong / 2D), 2D);
		double c = 2D * Math.atan2(Math.sqrt(a), Math.sqrt(1D - a));
		double d = _eQuatorialEarthRadius * c;

		return (int) (1000D * d);
	}

	public boolean hasVector(ArrayList<double[]> p_vectors, double[] p_vector)
	{
		for (int i = 0; i < p_vectors.size(); i++) {
			double[] v = p_vectors.get(i);
			if (v[0] == p_vector[0] && v[1] == p_vector[1]
					&& v[2] == p_vector[2] && v[3] == p_vector[3]) {
				return true;
			}
		}
		return false;
	}

	public static void displayPoint(double[] p)
	{
		System.out.println("Point(" + p[1] + "," + p[0] + ")");
	}

	public static void displayVector(double[] a, double[] b)
	{
		System.out.println("Vector: [" + a[1] + "," + a[0] + "] -> [" + b[1]
				+ "," + b[0] + "]");
	}

	// test
	public static void main(String[] args) throws Exception
	{
		
	}

	public static double st_distance_sphere(double lng1, double lat1,
			double lng2, double lat2)
	{
		try {
			ResultSet rs = null;
			try {
				stmt_distance_sphere.setObject(1, lng1);
				stmt_distance_sphere.setObject(2, lat1);
				stmt_distance_sphere.setObject(3, lng2);
				stmt_distance_sphere.setObject(4, lat2);
				rs = stmt_distance_sphere.executeQuery();
				rs.next();
				return rs.getDouble(1);
			} finally {
				if (rs != null)
					rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}
}