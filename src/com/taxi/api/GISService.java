package com.taxi.api;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.geojson.Polygon;

import com.db.DBUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gis.model.GeometryType;
import com.gis.model.Trip;

@Path("")
public class GISService extends GISBaseService {

	@GET
	@Path("gis/GetSegment")
	public Response getSegment(@QueryParam("segment_id") String segment_id)
			throws Exception
	{
		segment_id = segment_id.replaceAll(",", "");
		JSONObject res = new JSONObject();
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(
				"select taxi.get_segment(?)", Long.parseLong(segment_id));
		stmt.getConnection().setAutoCommit(false);
		ResultSet rs = stmt.executeQuery();
		if (rs.next())
			rs = (ResultSet) rs.getObject(1);
		FeatureCollection feature_collection = new FeatureCollection();
		Feature feature = new Feature();
		LineString line = new LineString();
		while (rs.next()) {
			line.add(new LngLatAlt(rs.getDouble("from_lng"), rs
					.getDouble("from_lat")));
			line.add(new LngLatAlt(rs.getDouble("to_lng"), rs
					.getDouble("to_lat")));
			feature.setProperty("from_node", rs.getLong("from_node"));
			feature.setProperty("to_node", rs.getLong("to_node"));
			feature.setProperty("section_id", rs.getLong("section_id"));
			feature.setProperty("way_id", rs.getLong("way_id"));
			feature.setProperty("way_name", rs.getString("way_name"));
		}
		closeResultSetResource(rs);
		feature.setGeometry(line);
		feature_collection.add(feature);
		res.put("result",
				new ObjectMapper().writeValueAsString(feature_collection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetSection")
	public Response getSection(@QueryParam("section_id") String id)
			throws Exception
	{
		JSONObject res = new JSONObject();
		FeatureCollection feature_collection = new FeatureCollection();
		Feature feature = new Feature();
		LineString line_string = new LineString();
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(
				"select taxi.get_section(?)", Long.parseLong(id));
		stmt.getConnection().setAutoCommit(false);
		ResultSet rs = stmt.executeQuery();
		if (rs.next())
			rs = (ResultSet) rs.getObject(1);
		while (rs.next()) {
			line_string.add(new LngLatAlt(rs.getDouble("from_lng"), rs
					.getDouble("from_lat")));
			line_string.add(new LngLatAlt(rs.getDouble("to_lng"), rs
					.getDouble("to_lat")));
			feature.setProperty("way_id", rs.getLong("way_id"));
			feature.setProperty("way_name", rs.getString("way_name"));
		}
		closeResultSetResource(rs);
		feature.setGeometry(line_string);
		feature_collection.add(feature);
		res.put("result",
				new ObjectMapper().writeValueAsString(feature_collection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetNode")
	public Response getNode(@QueryParam("node_id") String id) throws Exception
	{

		id = id.replaceAll("[\" {}\\n]", "");
		String id_csv[] = id.split(",");
		long ids[] = new long[id_csv.length];
		for (int i = 0; i < id_csv.length; ++i)
			ids[i] = Long.parseLong(id_csv[i]);
		JSONObject res = new JSONObject();
		FeatureCollection featureCollection = new FeatureCollection();
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement("select taxi.get_node(?)");
		stmt.getConnection().setAutoCommit(false);
		for (int i = 0; i < ids.length; ++i) {
			stmt.setObject(1, ids[i]);
			ResultSet rs_stmt = stmt.executeQuery();
			ResultSet rs_tags = null;
			ResultSet rs_node = null;
			if (rs_stmt.next()) {
				rs_node = (ResultSet) rs_stmt.getObject(1);
				rs_stmt.next();
				rs_tags = (ResultSet) rs_stmt.getObject(1);
				rs_stmt.close();
			}
			Feature feature = new Feature();
			featureCollection.add(feature);
			Point point = (Point) getGeometry(GeometryType.POINT, rs_node);
			feature.setGeometry(point);
			feature.setProperty("lng", point.getCoordinates().getLongitude());
			feature.setProperty("lat", point.getCoordinates().getLatitude());
			feature.setProperty("id", ids[i]);
			while (rs_tags.next())
				feature.setProperty(rs_tags.getString("key"),
						rs_tags.getString("value"));
			rs_node.close();
			rs_tags.close();
		}
		closeStatementResource(stmt);
		ObjectMapper mapper = new ObjectMapper();
		res.put("result", mapper.writeValueAsString(featureCollection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetWay")
	public Response getWay(@QueryParam("way_id") long id) throws Exception
	{
		JSONObject res = new JSONObject();
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement("select taxi.get_way(?)",
				id);
		stmt.getConnection().setAutoCommit(false);
		ResultSet rs_stmt = stmt.executeQuery();
		ResultSet rs_way = null, rs_tags = null;
		if (rs_stmt.next()) {
			rs_way = (ResultSet) rs_stmt.getObject(1);
			rs_stmt.next();
			rs_tags = (ResultSet) rs_stmt.getObject(1);
			rs_stmt.close();
		}
		FeatureCollection featureCollection = new FeatureCollection();
		Feature feature = new Feature();
		featureCollection.add(feature);
		feature.setGeometry(getGeometry(GeometryType.LINE_STRING, rs_way));
		while (rs_tags.next())
			feature.setProperty(rs_tags.getString("key"),
					rs_tags.getString("value"));
		rs_way.close();
		rs_tags.close();
		stmt.getConnection().commit();
		closeStatementResource(stmt);
		ObjectMapper mapper = new ObjectMapper();
		res.put("result", mapper.writeValueAsString(featureCollection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetWayByName")
	public Response getWayByName(@QueryParam("way_name") String way_name)
			throws Exception
	{
		JSONObject res = new JSONObject();
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(
				"select taxi.get_way_by_name(?)", way_name);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		FeatureCollection featureCollection = new FeatureCollection();
		Feature feature = new Feature();
		featureCollection.add(feature);
		feature.setGeometry(getGeometry(GeometryType.LINE_STRING, rs));
		stmt.getMoreResults();
		rs = stmt.getResultSet();
		while (rs.next())
			feature.setProperty(rs.getString("k"), rs.getString("v"));
		closeResultSetResource(rs);
		ObjectMapper mapper = new ObjectMapper();
		res.put("result", mapper.writeValueAsString(featureCollection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("report/GetSegmentsLngLat")
	public Response getSegmentLngLat(@QueryParam("segment_id") String segment_id)
			throws Exception
	{
		JSONObject res = new JSONObject();
		segment_id = segment_id.replaceAll("\\[", "");
		segment_id = segment_id.replaceAll("\\]", "");
		String segments[] = segment_id.split(",");
		PreparedStatement segment_info_stmt = DBUtil.getInstance().createSqlStatement("call nj_bus.get_segment_lng_lat(?)");
		StringBuilder sb = new StringBuilder(1 << 9);
		sb.append("LineString(");
		for (int i = 0; i < 1; ++i) {
			segment_info_stmt.setLong(1, Long.parseLong(segments[i]));
			segment_info_stmt.execute();
			ResultSet rs = segment_info_stmt.getResultSet();
			if (rs.next()) {
				sb.append(rs.getString("from_lng"));
				sb.append(' ');
				sb.append(rs.getString("from_lat"));
				sb.append(",");
				sb.append(rs.getString("to_lng"));
				sb.append(' ');
				sb.append(rs.getString("to_lat"));
			}
		}
		for (int i = 1; i < segments.length; ++i) {
			segment_info_stmt.setLong(1, Long.parseLong(segments[i]));
			segment_info_stmt.execute();
			ResultSet rs = segment_info_stmt.getResultSet();
			if (rs.next()) {
				sb.append(",");
				sb.append(rs.getString("from_lng"));
				sb.append(' ');
				sb.append(rs.getString("from_lat"));
				sb.append(",");
				sb.append(rs.getString("to_lng"));
				sb.append(' ');
				sb.append(rs.getString("to_lat"));
			}
		}
		sb.append(")");
		res.put("result", sb.toString());
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetAreaNodesAndLines")
	public Response getWayByName(@QueryParam("north") String p_north,
			@QueryParam("south") String p_south,
			@QueryParam("west") String p_west, @QueryParam("east") String p_east)
			throws Exception
	{
		JSONObject res = new JSONObject();
		// nodes and lines
		HashMap<String, JSONObject> nodes = new HashMap<String, JSONObject>();
		HashMap<String, JSONObject> lines = new HashMap<String, JSONObject>();

		String sqlStr = "select "
				+ "	T4.section_id as sec_id, T1.id as seg_id, T1.from_node as from_id, ST_X(T2.geom) as from_x, ST_Y(T2.geom) as from_y, T1.to_node as to_id, ST_X(T3.geom) as to_x, ST_Y(T3.geom) as to_y, "
				+ "	CASE  "
				+ "	    WHEN T1.from_node = T7.from_node THEN 'yes' "
				+ "	    ELSE 'no' "
				+ "	END as is_section_from, "
				+ "	CASE  "
				+ "	    WHEN T1.to_node = T7.to_node THEN 'yes' "
				+ "	    ELSE 'no' "
				+ "	END as is_section_to, "
				+ "	ST_Distance_Sphere(T2.geom, T3.geom) as segment_length "
				+ "from  "
				+ "	taxi.segments T1, nodes T2, nodes T3, taxi.segment_section T4, taxi.section_way T5, ways T6, taxi.sections T7 "
				+ "where "
				+ "	T1.from_node = T2.id and "
				+ "	T1.to_node = T3.id and "
				+ "	T1.id = T4.segment_id and "
				+ "	T4.section_id = T5.section_id and "
				+ "	T4.section_id = T7.id and "
				+ "	T5.way_id = T6.id and "
				+ "	T6.tags->'highway' in (select type from taxi.highway_types) and "
				+ "	? <= ST_X(T2.geom) and ST_X(T2.geom) <= ? and "
				+ "	? <= ST_Y(T3.geom) and ST_Y(T3.geom) <= ? ";
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sqlStr,
				Double.valueOf(p_west), Double.valueOf(p_east),
				Double.valueOf(p_south), Double.valueOf(p_north));
		ResultSet rs_stmt = stmt.executeQuery();

		while (rs_stmt.next()) {
			JSONObject line = new JSONObject();
			line.put("id", rs_stmt.getString("seg_id"));
			line.put("from_x", rs_stmt.getString("from_x"));
			line.put("from_y", rs_stmt.getString("from_y"));
			line.put("to_x", rs_stmt.getString("to_x"));
			line.put("to_y", rs_stmt.getString("to_y"));
			line.put("length", rs_stmt.getString("segment_length"));

			lines.put(line.getString("id"), line);

			if (!nodes.containsKey(rs_stmt.getString("from_id"))) {
				JSONObject node = new JSONObject();
				node.put("id", rs_stmt.getString("from_id"));
				node.put("x", rs_stmt.getString("from_x"));
				node.put("y", rs_stmt.getString("from_y"));
				node.put("is_section_node",
						rs_stmt.getString("is_section_from"));

				nodes.put(node.getString("id"), node);
			}

			if (!nodes.containsKey(rs_stmt.getString("to_id"))) {
				JSONObject node = new JSONObject();
				node.put("id", rs_stmt.getString("to_id"));
				node.put("x", rs_stmt.getString("to_x"));
				node.put("y", rs_stmt.getString("to_y"));
				node.put("is_section_node", rs_stmt.getString("is_section_to"));

				nodes.put(node.getString("id"), node);
			}
		}
		JSONObject obj = new JSONObject();
		JSONArray nodesArray = new JSONArray();
		JSONArray linesArray = new JSONArray();
		Iterator<Entry<String, JSONObject>> iter = nodes.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, JSONObject> entry = iter.next();
			nodesArray.put((JSONObject) entry.getValue());
		}
		iter = lines.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, JSONObject> entry = iter.next();
			linesArray.put((JSONObject) entry.getValue());
		}

		closeResultSetResource(rs_stmt);

		obj.put("nodes", nodesArray);
		obj.put("lines", linesArray);
		res.put("result", obj);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetShortestPathDijkstra")
	public Response getShortestPathDijkstra(
			@QueryParam("start") String p_start,
			@QueryParam("end") String p_end,
			@QueryParam("directed") String p_directed,
			@QueryParam("has_rcost") String p_has_rcost) throws Exception
	{
		JSONObject res = new JSONObject();
		JSONArray routes = new JSONArray();

		p_start = p_start.replaceAll("[\" {}\\n]", "");
		p_end = p_end.replaceAll("[\" {}\\n]", "");
		String start[] = p_start.split(",");
		String end[] = p_end.split(",");

		JSONObject originStart = new JSONObject();
		originStart.put("x", Double.parseDouble(start[0]));
		originStart.put("y", Double.parseDouble(start[1]));
		JSONObject originEnd = new JSONObject();
		originEnd.put("x", Double.parseDouble(end[0]));
		originEnd.put("y", Double.parseDouble(end[1]));

		JSONObject startJSON = getNearestSectionPoint(
				Double.parseDouble(start[0]), Double.parseDouble(start[1]));
		JSONObject endJSON = getNearestSectionPoint(Double.parseDouble(end[0]),
				Double.parseDouble(end[1]));

		if (!startJSON.has("node_id") || !endJSON.has("node_id")) {
			// nothing
		} else {
			String sql = "select " + "		T1.id as section_id, T4.* " + "from "
					+ "		taxi.sections T1, nodes T2, nodes T3, ( "
					+ "		select  " + " 		the_geom, " + "			T2.seq,  "
					+ "			T1.id as edge_id,  "
					+ "			T1.x1, T1.y1, T1.x2, T1.y2 " + "		from  "
					+ "			taxi.edges T1, ( "
					+ "			SELECT seq, id1 AS node, id2 AS edge, cost "
					+ "			FROM  " + "				pgr_dijkstra(' " + "					SELECT id, "
					+ "						source::integer, " + "						target::integer, "
					+ "					len::double precision AS cost, "
					+ "					rlen::double precision AS reverse_cost "
					+ "					FROM taxi.edges', ?, ?, ?, ?) " + "	)T2 "
					+ "	where  " + "		T2.edge = T1.id " + "	order by "
					+ "		T2.seq " + "	) T4 " + "where "
					+ "	T1.from_node = T2.id and " + "	T1.to_node = T3.id and "
					+ "	T4.x1 = st_x(T2.geom) and "
					+ "	T4.y1 = st_y(T2.geom) and "
					+ "	T4.x2 = st_x(T3.geom) and " + "	T4.y2 = st_y(T3.geom)"
					+ "order by T4.seq ";
			PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sql,
					Integer.parseInt(startJSON.get("node_id").toString()),
					Integer.parseInt(endJSON.get("node_id").toString()),
					Boolean.parseBoolean(p_directed),
					Boolean.parseBoolean(p_has_rcost));
			// stmt.getConnection().setAutoCommit(false);
			ResultSet rs_secs = stmt.executeQuery();

			String routeIndex = "0";
			JSONObject route = new JSONObject();
			route.put("routeId", routeIndex);
			route.put("sections", new JSONArray());

			while (rs_secs.next()) {
				JSONObject line = new JSONObject();
				line.put("section_id", rs_secs.getString("section_id"));
				line.put("seq", rs_secs.getString("seq"));
				line.put("route_id", routeIndex);
				line.put("edge_id", rs_secs.getString("edge_id"));
				line.put("x1", rs_secs.getString("x1"));
				line.put("y1", rs_secs.getString("y1"));
				line.put("x2", rs_secs.getString("x2"));
				line.put("y2", rs_secs.getString("y2"));

				route.getJSONArray("sections").put(line);
			}
			routes.put(route);

			rs_secs.close();
			closeStatementResource(stmt);
		}

		res.put("originStart", originStart);
		res.put("originEnd", originEnd);
		res.put("start", startJSON);
		res.put("end", endJSON);
		res.put("routes", routes);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetAStar")
	public Response getAStar(@QueryParam("start") String p_start,
			@QueryParam("end") String p_end,
			@QueryParam("directed") String p_directed,
			@QueryParam("has_rcost") String p_has_rcost) throws Exception
	{
		JSONObject res = new JSONObject();
		JSONArray routes = new JSONArray();

		p_start = p_start.replaceAll("[\" {}\\n]", "");
		p_end = p_end.replaceAll("[\" {}\\n]", "");
		String start[] = p_start.split(",");
		String end[] = p_end.split(",");

		JSONObject originStart = new JSONObject();
		originStart.put("x", Double.parseDouble(start[0]));
		originStart.put("y", Double.parseDouble(start[1]));
		JSONObject originEnd = new JSONObject();
		originEnd.put("x", Double.parseDouble(end[0]));
		originEnd.put("y", Double.parseDouble(end[1]));

		JSONObject startJSON = getNearestSectionPoint(
				Double.parseDouble(start[0]), Double.parseDouble(start[1]));
		JSONObject endJSON = getNearestSectionPoint(Double.parseDouble(end[0]),
				Double.parseDouble(end[1]));

		if (!startJSON.has("node_id") || !endJSON.has("node_id")) {
			// nothing
		} else {
			String sql = "select " + "	T1.id as section_id, T4.* " + "from "
					+ "	taxi.sections T1, nodes T2, nodes T3, ( " + "		select "
					+ "			T2.seq,  " + "			T1.id as edge_id,  "
					+ "			T1.x1, T1.y1, T1.x2, T1.y2  " + "		from  "
					+ "			taxi.edges T1, ( "
					+ "				SELECT seq, id1 AS node, id2 AS edge, cost  "
					+ "				FROM  " + "					pgr_astar(' " + "						SELECT id, "
					+ "							source::integer, " + "							target::integer, "
					+ "							len::double precision AS cost, "
					+ "							x1, y1, x2, y2, "
					+ "							rlen::double precision AS reverse_cost "
					+ "					FROM taxi.edges', ?, ?, ?, ?) " + "			) T2  "
					+ "		where  " + "			T2.edge = T1.id " + "		order by "
					+ "			T2.seq" + "	) T4 " + "where "
					+ "	T1.from_node = T2.id and " + "	T1.to_node = T3.id and "
					+ "	T4.x1 = st_x(T2.geom) and "
					+ "	T4.y1 = st_y(T2.geom) and "
					+ "	T4.x2 = st_x(T3.geom) and " + "	T4.y2 = st_y(T3.geom)"
					+ "order by T4.seq ";
			PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sql,
					Integer.parseInt(startJSON.get("node_id").toString()),
					Integer.parseInt(endJSON.get("node_id").toString()),
					Boolean.parseBoolean(p_directed),
					Boolean.parseBoolean(p_has_rcost));
			stmt.getConnection().setAutoCommit(false);
			ResultSet rs_secs = stmt.executeQuery();

			String routeIndex = "0";
			JSONObject route = new JSONObject();
			route.put("routeId", routeIndex);
			route.put("sections", new JSONArray());

			while (rs_secs.next()) {
				JSONObject line = new JSONObject();
				line.put("section_id", rs_secs.getString("section_id"));
				line.put("seq", rs_secs.getString("seq"));
				line.put("route_id", routeIndex);
				line.put("edge_id", rs_secs.getString("edge_id"));
				line.put("x1", rs_secs.getString("x1"));
				line.put("y1", rs_secs.getString("y1"));
				line.put("x2", rs_secs.getString("x2"));
				line.put("y2", rs_secs.getString("y2"));

				route.getJSONArray("sections").put(line);
			}
			routes.put(route);

			rs_secs.close();
			closeStatementResource(stmt);
		}

		res.put("originStart", originStart);
		res.put("originEnd", originEnd);
		res.put("start", startJSON);
		res.put("end", endJSON);
		res.put("routes", routes);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetRouteRecommendation")
	public Response getRouteRecommendation(@QueryParam("start") String p_start,
			@QueryParam("end") String p_end,
			@QueryParam("radius") double p_radius,
			@QueryParam("extend") double p_extend,
			@QueryParam("representation") short p_representation,
			@QueryParam("directed") boolean p_directed,
			@QueryParam("has_rcost") boolean p_has_rcost) throws Exception
	{
		JSONObject res = new JSONObject();

		p_start = p_start.replaceAll("[\" {}\\n]", "");
		p_end = p_end.replaceAll("[\" {}\\n]", "");
		String start[] = p_start.split(",");
		String end[] = p_end.split(",");
		double o_lng = Double.parseDouble(start[0]), o_lat = Double
				.parseDouble(start[1]);
		double d_lng = Double.parseDouble(end[0]), d_lat = Double
				.parseDouble(end[1]);
		int limit = 1000;

		JSONObject originStart = new JSONObject();
		originStart.put("x", o_lng);
		originStart.put("y", o_lat);
		JSONObject originEnd = new JSONObject();
		originEnd.put("x", d_lng);
		originEnd.put("y", d_lat);

		JSONObject startJSON = getNearestSectionPoint(
				Double.parseDouble(start[0]), Double.parseDouble(start[1]));
		JSONObject endJSON = getNearestSectionPoint(Double.parseDouble(end[0]),
				Double.parseDouble(end[1]));
		String sql = "select count(*) from "
				+ "taxi.get_trips_with_od(?, ?, ?, ?, ?, ?)";
		PreparedStatement stmt_trip_counts = DBUtil.getInstance().createSqlStatement(sql);

		boolean use_history = true;
		stmt_trip_counts.setObject(1, o_lng);
		stmt_trip_counts.setObject(2, o_lat);
		stmt_trip_counts.setObject(3, d_lng);
		stmt_trip_counts.setObject(4, d_lat);
		stmt_trip_counts.setObject(5, limit);
		stmt_trip_counts.setObject(6, p_radius);
		ResultSet rs_trip_counts = stmt_trip_counts.executeQuery();
		if (rs_trip_counts.next() && rs_trip_counts.getLong(1) < 5) {
			use_history = false;
		}
		rs_trip_counts.close();
		if (!startJSON.has("node_id") || !endJSON.has("node_id")) {
			// nothing
		} else if (use_history) {
			switch (p_representation) {
			case 1:
				res = getRouteRecommendationByRectangle(o_lng, o_lat, d_lng,
						d_lat, limit, p_radius, (int) p_extend);
				break;
			case 2:
				res = getRouteRecommendationByInterseciton(o_lng, o_lat, d_lng,
						d_lat, limit, p_radius);
				break;
			default:
				break;
			}
		} else {
			System.out.println("Shortest path:");
			sql = "select id1, id2 "
					+ "from pgr_dijkstra('"
					+ "select "
					+ "id::int, source::int, target::int, len as cost, rlen as reverse_cost "
					+ "from taxi.edges', " + "?, ?, ?, ?)";
			PreparedStatement stmt_cost_result = stmt_trip_counts
					.getConnection().prepareStatement(sql);
			sql = "select st_asText(the_geom) " + "from taxi.edges "
					+ "where id = ?";
			PreparedStatement stmt_edge = stmt_trip_counts.getConnection()
					.prepareStatement(sql);
			stmt_trip_counts.close();
			stmt_cost_result.setObject(1, startJSON.getInt("node_id"));
			stmt_cost_result.setObject(2, endJSON.getInt("node_id"));
			stmt_cost_result.setObject(3, p_directed);
			stmt_cost_result.setObject(4, p_has_rcost);
			JSONArray routes = new JSONArray();
			JSONObject route = new JSONObject();
			JSONArray vertices = new JSONArray();
			JSONArray lines = new JSONArray();
			route.put("linestring", lines);
			route.put("vertex", vertices);
			route.put("trip_id", 1);
			ResultSet rs_cost_result = stmt_cost_result.executeQuery();
			ResultSet rs_edge;
			while (rs_cost_result.next()) {
				if (rs_cost_result.getLong(2) == -1)
					break;
				stmt_edge.setObject(1, rs_cost_result.getLong(2));
				rs_edge = stmt_edge.executeQuery();
				rs_edge.next();
				vertices.put(rs_cost_result.getLong(1));
				lines.put(rs_edge.getString(1));
			}
			stmt_edge.close();
			rs_cost_result.close();
			closeStatementResource(stmt_cost_result);
			routes.put(route);
			res.put("trajectory", routes);
			res.put("representation", 2);
		}

		res.put("originStart", originStart);
		res.put("originEnd", originEnd);
		res.put("start", startJSON);
		res.put("end", endJSON);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetKSP")
	public Response getKSP(@QueryParam("start") String p_start,
			@QueryParam("end") String p_end, @QueryParam("k") String p_kpaths,
			@QueryParam("has_rcost") String p_has_rcost) throws Exception
	{
		JSONObject res = new JSONObject();
		JSONArray routes = new JSONArray();

		p_start = p_start.replaceAll("[\" {}\\n]", "");
		p_end = p_end.replaceAll("[\" {}\\n]", "");
		String start[] = p_start.split(",");
		String end[] = p_end.split(",");

		JSONObject originStart = new JSONObject();
		originStart.put("x", Double.parseDouble(start[0]));
		originStart.put("y", Double.parseDouble(start[1]));
		JSONObject originEnd = new JSONObject();
		originEnd.put("x", Double.parseDouble(end[0]));
		originEnd.put("y", Double.parseDouble(end[1]));

		JSONObject startJSON = getNearestSectionPoint(
				Double.parseDouble(start[0]), Double.parseDouble(start[1]));
		JSONObject endJSON = getNearestSectionPoint(Double.parseDouble(end[0]),
				Double.parseDouble(end[1]));

		if (!startJSON.has("node_id") || !endJSON.has("node_id")) {
			// nothing
		} else {
			String sql = "select "
					+ "	T1.id as section_id, T4.* "
					+ "from "
					+ "	taxi.sections T1, nodes T2, nodes T3, ( "
					+ "		select "
					+ "				T2.seq, "
					+ "				T2.route as route_id, "
					+ "				T1.id as edge_id,  "
					+ "				T1.x1, T1.y1, T1.x2, T1.y2 "
					+ "		from  "
					+ "			taxi.edges T1, ( "
					+ "				SELECT seq, id1 AS route, id2 AS node, id3 AS edge, cost  "
					+ "				FROM  " + "					pgr_ksp(' " + "						SELECT id, "
					+ "							source::integer, " + "							target::integer, "
					+ "							len::double precision AS cost, "
					+ "							rlen::double precision AS reverse_cost "
					+ "					FROM taxi.edges', ?, ?, ?, ?) " + "			) T2  "
					+ "		where  " + "			T2.edge = T1.id " + "		order by "
					+ "			T2.seq" + "	) T4 " + "where "
					+ "	T1.from_node = T2.id and " + "	T1.to_node = T3.id and "
					+ "	T4.x1 = st_x(T2.geom) and "
					+ "	T4.y1 = st_y(T2.geom) and "
					+ "	T4.x2 = st_x(T3.geom) and " + "	T4.y2 = st_y(T3.geom)"
					+ "order by T4.seq ";
			System.out.println(sql);
			PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sql,
					Integer.parseInt(startJSON.get("node_id").toString()),
					Integer.parseInt(endJSON.get("node_id").toString()),
					Integer.parseInt(p_kpaths),
					Boolean.parseBoolean(p_has_rcost));
			stmt.getConnection().setAutoCommit(false);
			ResultSet rs_secs = stmt.executeQuery();

			String routeIndex = "0";
			JSONObject route = new JSONObject();
			route.put("routeId", routeIndex);
			route.put("sections", new JSONArray());

			while (rs_secs.next()) {
				JSONObject line = new JSONObject();
				line.put("seq", rs_secs.getString("seq"));
				line.put("route_id", rs_secs.getString("route_id"));
				line.put("edge_id", rs_secs.getString("edge_id"));
				line.put("section_id", rs_secs.getString("section_id"));
				line.put("x1", rs_secs.getString("x1"));
				line.put("y1", rs_secs.getString("y1"));
				line.put("x2", rs_secs.getString("x2"));
				line.put("y2", rs_secs.getString("y2"));

				if (rs_secs.getString("route_id").equals(routeIndex)) {
					route.getJSONArray("sections").put(line);
				} else {
					routes.put(route);
					routeIndex = rs_secs.getString("route_id");

					route = new JSONObject();
					route.put("routeId", routeIndex);
					route.put("sections", new JSONArray());
					route.getJSONArray("sections").put(line);
				}
			}
			routes.put(route);

			rs_secs.close();
			closeStatementResource(stmt);
		}

		res.put("originStart", originStart);
		res.put("originEnd", originEnd);
		res.put("start", startJSON);
		res.put("end", endJSON);
		res.put("routes", routes);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetAlignedSegments")
	public Response getAlignedSegments(@QueryParam("uid") String uid,
			@QueryParam("begin") Timestamp begin_time,
			@QueryParam("end") Timestamp end_time) throws Exception
	{
		JSONObject res = new JSONObject();
		JSONArray segment_ids = new JSONArray();
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(
				"select lng, lat, timestamp from taxi.get_filtered_gps(?, ?, ?)"
						+ "as(lng double precision," + "lat double precision,"
						+ "timestamp timestamp,"
						+ "point geometry(Point, 4326))", uid, begin_time,
				end_time);
		ResultSet rs = stmt.executeQuery();
		FeatureCollection feature_collection = new FeatureCollection();
		while (rs.next()) {
			Feature feature = new Feature();
			Point point = new Point(rs.getDouble("lng"), rs.getDouble("lat"));
			feature.setGeometry(point);
			feature.setProperty("seq", rs.getRow());
			feature_collection.add(feature);
		}
		System.out.println("size: " + feature_collection.getFeatures().size());
		closeResultSetResource(rs);
		List<Feature> features = feature_collection.getFeatures();
		stmt = DBUtil.getInstance().createSqlStatement("select * from taxi.get_aligned_segments(?, ?, ?)as "
				+ "(id bigint," + "from_node bigint," + "to_node bigint)");
		// stmt.getConnection().setAutoCommit(false);
		if (features.size() == 0)
			return null;
		Point pre_point = (Point) features.get(0).getGeometry();
		int i = 1;
		while (i < features.size()
				&& ((Point) features.get(i).getGeometry()).getCoordinates()
						.getLongitude() == pre_point.getCoordinates()
						.getLongitude()
				&& ((Point) features.get(i).getGeometry()).getCoordinates()
						.getLatitude() == pre_point.getCoordinates()
						.getLatitude()) {
			pre_point = (Point) features.get(i).getGeometry();
			++i;
		}
		org.postgis.PGgeometry pgeom = new org.postgis.PGgeometry();
		org.postgis.Point ppoint = new org.postgis.Point();
		for (; i < features.size(); ++i) {
			Point cur_point = (Point) features.get(i).getGeometry();
			if (cur_point.getCoordinates().getLongitude() == pre_point
					.getCoordinates().getLongitude()
					&& cur_point.getCoordinates().getLatitude() == pre_point
							.getCoordinates().getLatitude())
				continue;
			ppoint.setX(cur_point.getCoordinates().getLongitude());
			ppoint.setY(cur_point.getCoordinates().getLatitude());
			System.out.println("assign point " + (i + 1));
			System.out.println(cur_point.getCoordinates().getLongitude() + " "
					+ cur_point.getCoordinates().getLatitude());
			Map.Entry<Double, Double> dir = getDirection(pre_point, cur_point);
			pre_point = cur_point;
			System.out.println(dir);
			pgeom.setGeometry(ppoint);
			stmt.setObject(1, pgeom);
			stmt.setObject(2, dir.getKey());
			stmt.setObject(3, dir.getValue());
			rs = stmt.executeQuery();
			if (rs.next()) {
				features.get(i).setProperty("segment", rs.getLong("id"));
				segment_ids.put(rs.getLong("id"));
				rs.close();
			}
		}
		closeStatementResource(stmt);
		res.put("points",
				new ObjectMapper().writeValueAsString(feature_collection));
		res.put("segments", segment_ids);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetHeatmap")
	public Response getHeatmap(@QueryParam("northwest") String p_northwest,
			@QueryParam("southeast") String p_southeast,
			@QueryParam("fromTime") String p_fromTime,
			@QueryParam("toTime") String p_toTime,
			@QueryParam("type") String p_type) throws Exception
	{
		JSONObject res = new JSONObject();
		JSONArray points = new JSONArray();

		p_northwest = p_northwest.replaceAll("[\" {}\\n]", "");
		p_southeast = p_southeast.replaceAll("[\" {}\\n]", "");
		p_fromTime = p_fromTime.replaceAll("[\" {}\\n]", "") + " 00:00:00";
		p_toTime = p_toTime.replaceAll("[\" {}\\n]", "") + " 23:59:59";

		String northwest[] = p_northwest.split(",");
		String southeast[] = p_southeast.split(",");
		double north = Double.parseDouble(northwest[0]);
		double west = Double.parseDouble(northwest[1]);
		double south = Double.parseDouble(southeast[0]);
		double east = Double.parseDouble(southeast[1]);

		String sql = "";
		if (p_type.equals("O")) {
			sql = "select " + "	st_x(o_point) as lng, st_y(o_point) as lat "
					+ "from " + "	taxi.trips_od " + "where "
					+ " 	st_x(o_point) >= ? and " + "	st_x(o_point) <= ? and "
					+ "	st_y(o_point) >= ? and " + "	st_y(o_point) <= ? and "
					+ "	o_time >= '" + p_fromTime + "' and " + "	d_time <= '"
					+ p_toTime + "'";
		} else if (p_type.equals("D")) {
			sql = "select " + "	st_x(d_point) as lng, st_y(d_point) as lat "
					+ "from " + "	taxi.trips_od " + "where "
					+ " 	st_x(d_point) >= ? and " + "	st_x(d_point) <= ? and "
					+ "	st_y(d_point) >= ? and " + "	st_y(d_point) <= ? and "
					+ "	o_time >= '" + p_fromTime + "' and " + "	d_time <= '"
					+ p_toTime + "'";
		}

		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sql, west, east, south,
				north);
		ResultSet rs_stmt = stmt.executeQuery();
		JSONObject p = null;
		while (rs_stmt.next()) {
			double lng = Double.parseDouble(rs_stmt.getString(1));
			double lat = Double.parseDouble(rs_stmt.getString(2));
			p = new JSONObject();
			p.put("lat", lat);
			p.put("lng", lng);
			p.put("count", 1);
			points.put(p);
		}
		rs_stmt.close();
		closeStatementResource(stmt);

		res.put("points", points);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	public Map.Entry<Double, Double> getDirection(List<Point> points)
	{
		double v_x = 0, v_y = 0;
		Iterator<Point> pre = points.iterator();
		Point pre_point = null, post_point;
		if (pre.hasNext())
			pre_point = pre.next();
		while (pre.hasNext()) {
			post_point = pre.next();
			double v_cur_x = post_point.getCoordinates().getLongitude()
					- pre_point.getCoordinates().getLongitude();
			double v_cur_y = post_point.getCoordinates().getLatitude()
					- pre_point.getCoordinates().getLatitude();
			double v_temp = Math.sqrt(v_cur_x * v_cur_x + v_cur_y * v_cur_y);
			v_cur_x = v_cur_x / v_temp;
			v_cur_y = v_cur_y / v_temp;
			v_x += v_cur_x;
			v_y += v_cur_y;
			v_temp = Math.sqrt(v_x * v_x + v_y * v_y);
			v_x /= v_temp;
			v_y /= v_temp;
			pre_point = post_point;
		}
		return new AbstractMap.SimpleEntry<Double, Double>(v_x, v_y);
	}

	public Map.Entry<Double, Double> getDirection(Point pre, Point post)
	{
		double v_cur_x = post.getCoordinates().getLongitude()
				- pre.getCoordinates().getLongitude();
		double v_cur_y = post.getCoordinates().getLatitude()
				- pre.getCoordinates().getLatitude();
		double v_temp = Math.sqrt(v_cur_x * v_cur_x + v_cur_y * v_cur_y);
		v_cur_x = v_cur_x / v_temp;
		v_cur_y = v_cur_y / v_temp;
		return new AbstractMap.SimpleEntry<Double, Double>(v_cur_x, v_cur_y);
	}

	@GET
	@Path("gis/GetODTrajectory")
	public Response getODTrajectory(@QueryParam("start") String p_start,
			@QueryParam("end") String p_end,
			@QueryParam("radius") double p_radius,
			@QueryParam("type") String p_type) throws Exception
	{
		// request type
		/*
		 * 1. both have start and end 2. only have start 3. only have end
		 */
		JSONObject res = new JSONObject();
		FeatureCollection feature_collection = new FeatureCollection();

		p_start = p_start.replaceAll("[\" {}\\n]", "");
		p_end = p_end.replaceAll("[\" {}\\n]", "");
		String start[] = p_start.split(",");
		String end[] = p_end.split(",");
		double start_lat = 0;
		double start_lng = 0;
		double end_lat = 0;
		double end_lng = 0;

		String sql = "";
		switch (Integer.parseInt(p_type)) {
		case 1:
			start_lng = Double.parseDouble(start[0]);
			start_lat = Double.parseDouble(start[1]);
			end_lng = Double.parseDouble(end[0]);
			end_lat = Double.parseDouble(end[1]);
			sql = "select taxi.get_trips_with_od(?, ?, ?, ?, ?) as id";
			break;
		case 2:
			start_lat = Double.parseDouble(start[0]);
			start_lng = Double.parseDouble(start[1]);
			sql = "select "
					+ "	T1.id as driver , st_x(T1.point) as lng, st_y(T1.point) as lat, T1.timestamp,  "
					+ "	CASE  " + "	    WHEN T1.point = T2.o_point THEN 'o' "
					+ "            WHEN T1.point = T2.d_point THEN 'd' "
					+ "            ELSE 'm' " + "        END as flag "
					+ "from " + "	taxi.gps_raw T1, " + "	(select " + "		* "
					+ "	from " + "		taxi.trips_od " + "	where "
					+ "		st_distance_sphere(st_makepoint(" + start_lng + ", "
					+ start_lat + "), o_point) < " + p_radius + " " + "	) T2 "
					+ "where " + "	T1.id = T2.uid and "
					+ "	T1.timestamp >= T2.o_time and "
					+ "	T1.timestamp <= T2.d_time";
			break;
		case 3:
			end_lat = Double.parseDouble(end[0]);
			end_lng = Double.parseDouble(end[1]);
			sql = "select "
					+ "	T1.id as driver , st_x(T1.point) as lng, st_y(T1.point) as lat, T1.timestamp, "
					+ "	CASE  " + "	    WHEN T1.point = T2.o_point THEN 'o' "
					+ "            WHEN T1.point = T2.d_point THEN 'd' "
					+ "            ELSE 'm' " + "        END as flag "
					+ "from " + "	taxi.gps_raw T1, " + "	(select " + "		* "
					+ "	from " + "		taxi.trips_od " + "	where "
					+ "		st_distance_sphere(st_makepoint(" + end_lng + ", "
					+ end_lat + "), d_point) < " + p_radius + " " + "	) T2 "
					+ "where " + "	T1.id = T2.uid and "
					+ "	T1.timestamp >= T2.o_time and "
					+ "	T1.timestamp <= T2.d_time";
			break;
		}

		PreparedStatement stmt_trip_ids = DBUtil.getInstance().createSqlStatement(sql);
		PreparedStatement stmt_trip_points = stmt_trip_ids.getConnection()
				.prepareStatement(
						"select st_x(point) lng, st_y(point) lat "
								+ "from taxi.gps_raw " + "where trip_id = ?"
								+ "order by timestamp");
		stmt_trip_ids.setObject(1, start_lng);
		stmt_trip_ids.setObject(2, start_lat);
		stmt_trip_ids.setObject(3, end_lng);
		stmt_trip_ids.setObject(4, end_lat);
		stmt_trip_ids.setObject(5, p_radius);
		ResultSet rs_trip_ids = stmt_trip_ids.executeQuery();
		ResultSet rs_trip_points;
		while (rs_trip_ids.next()) {
			LineString line = new LineString();
			Feature feature = new Feature();
			feature.setGeometry(line);
			stmt_trip_points.setObject(1, rs_trip_ids.getLong("id"));
			rs_trip_points = stmt_trip_points.executeQuery();
			if (rs_trip_points.next()) {
				line.add(new LngLatAlt(rs_trip_points.getDouble("lng"),
						rs_trip_points.getDouble("lat")));
			}
			while (rs_trip_points.next()) {
				line.add(new LngLatAlt(rs_trip_points.getDouble("lng"),
						rs_trip_points.getDouble("lat")));
			}
			rs_trip_points.close();
			feature_collection.add(feature);
		}
		rs_trip_ids.close();
		closeStatementResource(stmt_trip_ids);

		res.put("trajectoryNum", feature_collection.getFeatures().size());
		res.put("trajectory",
				new ObjectMapper().writeValueAsString(feature_collection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetODTrajectory2")
	public Response getODTrajectory2(@QueryParam("start") String p_start,
			@QueryParam("end") String p_end,
			@QueryParam("radius") double p_radius,
			@QueryParam("limit") int p_limit,
			@QueryParam("type") String p_type, @QueryParam("od") boolean p_od)
			throws Exception
	{
		// request type
		/*
		 * 1. both have start and end 2. only have start 3. only have end
		 */
		JSONObject res = new JSONObject();
		FeatureCollection feature_collection = new FeatureCollection();

		p_start = p_start.replaceAll("[\" {}\\n]", "");
		p_end = p_end.replaceAll("[\" {}\\n]", "");
		String start[] = p_start.split(",");
		String end[] = p_end.split(",");
		double start_lat = 0;
		double start_lng = 0;
		double end_lat = 0;
		double end_lng = 0;

		String sql = "";
		switch (Integer.parseInt(p_type)) {
		case 1:
			start_lng = Double.parseDouble(start[0]);
			start_lat = Double.parseDouble(start[1]);
			end_lng = Double.parseDouble(end[0]);
			end_lat = Double.parseDouble(end[1]);
			if (p_od)
				sql = "select "
						+ "taxi.get_trips_with_od(?, ?, ?, ?, ?, ?) as id";
			else
				sql = "select "
						+ "taxi.get_trips_across(?, ?, ?, ?, ?, ?) as id";
			break;
		}

		PreparedStatement stmt_trip_ids = DBUtil.getInstance().createSqlStatement(sql);
		PreparedStatement stmt_trip_points = stmt_trip_ids.getConnection()
				.prepareStatement(
						"select st_x(point) lng, st_y(point) lat "
								+ "from taxi.gps_raw " + "where trip_id = ?"
								+ "order by timestamp");
		stmt_trip_ids.setObject(1, start_lng);
		stmt_trip_ids.setObject(2, start_lat);
		stmt_trip_ids.setObject(3, end_lng);
		stmt_trip_ids.setObject(4, end_lat);
		stmt_trip_ids.setObject(5, p_limit);
		stmt_trip_ids.setObject(6, p_radius);
		ResultSet rs_trip_ids = stmt_trip_ids.executeQuery();
		ResultSet rs_trip_points = null;
		while (rs_trip_ids.next()) {
			LineString line = new LineString();
			Feature feature = new Feature();
			feature.setGeometry(line);
			feature.setProperty("trip_id", rs_trip_ids.getLong("id"));
			stmt_trip_points.setObject(1, rs_trip_ids.getLong("id"));
			rs_trip_points = stmt_trip_points.executeQuery();
			if (rs_trip_points.next()) {
				line.add(new LngLatAlt(rs_trip_points.getDouble("lng"),
						rs_trip_points.getDouble("lat")));
			}
			while (rs_trip_points.next()) {
				line.add(new LngLatAlt(rs_trip_points.getDouble("lng"),
						rs_trip_points.getDouble("lat")));
			}
			rs_trip_points.close();
			feature_collection.add(feature);
		}
		rs_trip_ids.close();
		closeStatementResource(stmt_trip_ids);

		res.put("trajectoryNum", feature_collection.getFeatures().size());
		res.put("trajectory",
				new ObjectMapper().writeValueAsString(feature_collection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetTripTrajectory")
	public Response getTripTrajectory(@QueryParam("trip_id") Long p_trip_id,
			@QueryParam("augmented") boolean p_augmented,
			@QueryParam("assign") boolean p_raw) throws Exception
	{
		JSONObject res = new JSONObject();
		FeatureCollection feature_collection = new FeatureCollection();
		int seq = 0;
		if (p_raw) {
			PreparedStatement stmt_gps = DBUtil.getInstance().createSqlStatement("select "
					+ "st_x(point) lng," + "st_y(point) lat "
					+ "from taxi.gps_raw " + "where trip_id = ?", p_trip_id);
			ResultSet rs_gps = stmt_gps.executeQuery();
			while (rs_gps.next()) {
				Feature feature = new Feature();
				Point point = new Point(rs_gps.getDouble(1),
						rs_gps.getDouble(2));
				feature.setProperty("lng", point.getCoordinates()
						.getLongitude());
				feature.setProperty("lat", point.getCoordinates().getLatitude());
				feature.setProperty("seq", ++seq);
				feature.setGeometry(point);
				feature_collection.add(feature);
			}
		} else {
			String sql = "select " + "taxi.get_trip_vertices(?, ?) as id";
			PreparedStatement stmt_trip_vs = DBUtil.getInstance().createSqlStatement(sql);
			PreparedStatement stmt_point = stmt_trip_vs.getConnection()
					.prepareStatement(
							"select " + "st_x(the_geom) lng,"
									+ "st_y(the_geom) lat "
									+ "from taxi.edges_vertices_pgr "
									+ "where id = ?");

			stmt_trip_vs.setObject(1, p_trip_id);
			stmt_trip_vs.setObject(2, p_augmented);

			ResultSet rs_vertices = stmt_trip_vs.executeQuery();
			ResultSet rs_point = null;
			while (rs_vertices.next()) {
				stmt_point.setObject(1, rs_vertices.getLong(1));
				rs_point = stmt_point.executeQuery();
				if (!rs_point.next())
					continue;
				Point point = new Point(rs_point.getDouble(1),
						rs_point.getDouble(2));
				rs_point.close();
				Feature feature = new Feature();
				feature.setGeometry(point);
				feature.setProperty("lng", point.getCoordinates()
						.getLongitude());
				feature.setProperty("lat", point.getCoordinates().getLatitude());
				feature.setProperty("seq", ++seq);
				feature.setProperty("id", rs_vertices.getLong(1));
				feature_collection.add(feature);
			}
			rs_vertices.close();
			closeStatementResource(stmt_trip_vs);
		}
		res.put("point_num", feature_collection.getFeatures().size());
		res.put("trajectory",
				new ObjectMapper().writeValueAsString(feature_collection));
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	@GET
	@Path("gis/GetAugmentSections")
	public Response getAugmentSections(@QueryParam("trip_id") long p_trip_id)
			throws Exception
	{
		JSONObject res = new JSONObject();

		// get sections
		String sql = "select "
				+ "	T1.seq, T2.id as section_id, st_astext(T2.the_geom) as wkt "
				+ "from " + "	(select  " + "		row_number() over () as seq, "
				+ "		T.get_trip_traces as id " + "	from  "
				+ "		(select taxi.get_trip_traces(?)) T " + "	) T1,  "
				+ "	taxi.edges T2 " + "where " + "	T1.id = T2.id "
				+ "order by " + "	T1.seq";
		PreparedStatement stmt = DBUtil.getInstance().createSqlStatement(sql, p_trip_id);
		stmt.getConnection().setAutoCommit(false);
		ResultSet rs_stmt = stmt.executeQuery();
		JSONArray sections = new JSONArray();
		while (rs_stmt.next()) {
			JSONObject sec = new JSONObject();
			sec.put("seq", rs_stmt.getLong("seq"));
			sec.put("section_id", rs_stmt.getLong("section_id"));
			sec.put("wkt", rs_stmt.getString("wkt"));
			sections.put(sec);
		}
		rs_stmt.close();

		// get gps points
		sql = "select " + "	st_astext(T2.point) as wkt " + "from "
				+ "	taxi.trips_od T1, taxi.gps_raw T2 " + "where "
				+ "	T1.id = ? and " + "	T1.uid = T2.id and "
				+ "	T1.o_time <= T2.timestamp and "
				+ "	T1.d_time >= T2.timestamp and " + "	T2.state = true "
				+ "order by " + "	T2.timestamp";

		stmt = DBUtil.getInstance().createSqlStatement(sql, p_trip_id);
		rs_stmt = stmt.executeQuery();
		JSONArray points = new JSONArray();
		int i = 0;
		while (rs_stmt.next()) {
			i++;
			JSONObject p = new JSONObject();
			p.put("seq", i);
			p.put("wkt", rs_stmt.getString("wkt"));
			points.put(p);
		}
		rs_stmt.close();
		closeStatementResource(stmt);

		res.put("points", points);
		res.put("sections", sections);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return buildResponse(res, "application/json", expires.getTime());
	}

	private JSONObject getRouteRecommendationByInterseciton(double o_lng,
			double o_lat, double d_lng, double d_lat, int limit, double radius)
			throws Exception
	{
		String sql = "select "
				+ "taxi.get_trips_with_od(?, ?, ?, ?, ?, ?) as id";
		PreparedStatement stmt_trip_ids = DBUtil.getInstance().createSqlStatement(sql);
		stmt_trip_ids.setObject(1, o_lng);
		stmt_trip_ids.setObject(2, o_lat);
		stmt_trip_ids.setObject(3, d_lng);
		stmt_trip_ids.setObject(4, d_lat);
		stmt_trip_ids.setObject(5, limit);
		stmt_trip_ids.setObject(6, radius);
		List<Trip> history_trips = new LinkedList<Trip>();
		sql = "select taxi.get_trip_vertices(?,?)";
		PreparedStatement stmt_trip_vertices = stmt_trip_ids.getConnection()
				.prepareStatement(sql);
		stmt_trip_vertices.setObject(2, true);

		System.out.println("routes generating...");
		ResultSet rs_trip_ids = stmt_trip_ids.executeQuery();
		ResultSet rs_trip_vs;
		while (rs_trip_ids.next()) {
			Trip trip = new Trip(rs_trip_ids.getLong(1));
			stmt_trip_vertices.setObject(1, trip.getId());
			rs_trip_vs = stmt_trip_vertices.executeQuery();
			while (rs_trip_vs.next()) {
				trip.add(rs_trip_vs.getLong(1));
			}
			rs_trip_vs.close();
			history_trips.add(trip);
		}
		stmt_trip_ids.close();
		System.out.println("routes generated");

		System.out.println("clustering...");
		int i = 0;
		List<List<Trip>> clusters = new ArrayList<List<Trip>>();
		while (!history_trips.isEmpty()) {
			Iterator<Trip> it = history_trips.iterator();
			Trip cur = it.next();
			it.remove();
			clusters.add(new ArrayList<Trip>());
			clusters.get(i).add(cur);
			while (it.hasNext()) {
				Trip next = it.next();
				Set<Long> inters = new HashSet<Long>(cur.getVertices());
				Set<Long> unions = new HashSet<Long>(cur.getVertices());
				unions.addAll(next.getVertices());
				inters.retainAll(next.getVertices());
				if (1.0 * inters.size() / unions.size() > 0.7) {
					clusters.get(i).add(next);
					it.remove();
				}
			}
			++i;
		}
		System.out.println("clusters generated");
		Collections.sort(clusters, new Comparator<List<Trip>>() {
			@Override
			public int compare(List<Trip> o1, List<Trip> o2)
			{
				if (o1.size() > o2.size())
					return -1;
				if (o1.size() == o2.size())
					return 0;
				return 1;
			}
		});

		limit = 3;
		i = 0;
		Iterator<List<Trip>> it = clusters.iterator();

		PreparedStatement stmt_edge = stmt_trip_vertices.getConnection()
				.prepareStatement(
						"select st_asText(the_geom) " + "from taxi.edges "
								+ "where (source = ? and " + "target = ?) or"
								+ "(source = ? and target=?)");
		ResultSet rs_edge = null;
		JSONObject res = new JSONObject();
		res.put("representation", 2);
		JSONArray routes = new JSONArray();
		res.put("trajectory", routes);
		int min_distance = clusters.iterator().next().iterator().next()
				.getVertices().size();
		while (it.hasNext()) {
			++i;
			if (i >= limit)
				break;
			List<Trip> trips = it.next();
			/**
			 * Exclude lengthy paths
			 */
			if (trips.get(0).getVertices().size() >= 1.5 * min_distance)
				break;
			JSONObject route = new JSONObject();
			route.put("vertices", new JSONArray(trips.get(0).getVertices()));
			route.put("trip_id", trips.get(0).getId());
			JSONArray lines = new JSONArray(
					trips.get(0).getVertices().size() / 2);
			route.put("linestring", lines);
			routes.put(route);
			System.out.println(trips.get(0).getId() + "\t"
					+ trips.get(0).getVertices());
			for (int j = 1; j < trips.get(0).getVertices().size(); ++j) {
				stmt_edge.setObject(1, trips.get(0).getVertices().get(j - 1));
				stmt_edge.setObject(2, trips.get(0).getVertices().get(j));
				stmt_edge.setObject(3, trips.get(0).getVertices().get(j));
				stmt_edge.setObject(4, trips.get(0).getVertices().get(j - 1));
				rs_edge = stmt_edge.executeQuery();
				if (rs_edge.next())
					lines.put(rs_edge.getString(1));
			}
		}
		closeResultSetResource(rs_edge);
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return res;
	}

	private JSONObject getRouteRecommendationByRectangle(double o_lng,
			double o_lat, double d_lng, double d_lat, int limit, double radius,
			int extend) throws Exception
	{
		String sql = "select "
				+ "taxi.get_trips_with_od(?, ?, ?, ?, ?, ?) as id";
		PreparedStatement stmt_trip_ids = DBUtil.getInstance().createSqlStatement(sql);
		stmt_trip_ids.setObject(1, o_lng);
		stmt_trip_ids.setObject(2, o_lat);
		stmt_trip_ids.setObject(3, d_lng);
		stmt_trip_ids.setObject(4, d_lat);
		stmt_trip_ids.setObject(5, limit);
		stmt_trip_ids.setObject(6, radius);
		List<Trip> history_trips = new LinkedList<Trip>();
		sql = "select * from taxi.get_trip_points(?) "
				+ "as (lng double precision, lat double precision)";
		PreparedStatement stmt_trip_points = stmt_trip_ids.getConnection()
				.prepareStatement(sql);
		sql = "select mileage " + "from taxi.trips_od " + "where id = ?";
		PreparedStatement stmt_trip_length = stmt_trip_ids.getConnection()
				.prepareStatement(sql);

		System.out.println("traces generating...");
		ResultSet rs_trip_ids = stmt_trip_ids.executeQuery();
		ResultSet rs_trip_points;
		while (rs_trip_ids.next()) {
			Trip trip = new Trip(rs_trip_ids.getLong(1));
			stmt_trip_length.setObject(1, trip.getId());
			rs_trip_points = stmt_trip_length.executeQuery();
			rs_trip_points.next();
			trip.setLength(rs_trip_points.getDouble(1));
			rs_trip_points.close();

			stmt_trip_points.setObject(1, trip.getId());
			rs_trip_points = stmt_trip_points.executeQuery();
			while (rs_trip_points.next()) {
				trip.add(new AbstractMap.SimpleEntry<Double, Double>(
						rs_trip_points.getDouble(1), rs_trip_points
								.getDouble(2)));
			}
			rs_trip_points.close();
			history_trips.add(trip);
		}
		rs_trip_ids.close();
		stmt_trip_points.close();
		stmt_trip_ids.close();
		stmt_trip_length.close();
		closeStatementResource(stmt_trip_ids);
		System.out.println("traces generated: " + history_trips.size());
		System.out.println("clustering...");
		int i = 0;
		List<List<Trip>> clusters = new ArrayList<List<Trip>>();
		while (!history_trips.isEmpty()) {
			Iterator<Trip> it = history_trips.iterator();
			Trip cur = it.next();
			it.remove();
			clusters.add(new ArrayList<Trip>());
			clusters.get(i).add(cur);
			while (it.hasNext()) {
				Trip next = it.next();
				if (cur.similarityBetween(next, extend) >= 0.7) {
					clusters.get(i).add(next);
					it.remove();
				}
			}
			++i;
		}
		Collections.sort(clusters, new Comparator<List<Trip>>() {
			@Override
			public int compare(List<Trip> o1, List<Trip> o2)
			{
				if (o1.size() > o2.size())
					return -1;
				if (o1.size() == o2.size())
					return 0;
				return 1;
			}
		});
		System.out.println("clusters generated: ");
		for (List<Trip> list : clusters)
			System.out.print(list.size() + " ");

		limit = 3;
		i = 0;
		Iterator<List<Trip>> it = clusters.iterator();

		JSONObject res = new JSONObject();
		ObjectMapper objectMapper = new ObjectMapper();
		res.put("representation", 1);
		JSONArray routes = new JSONArray();
		res.put("trajectory", routes);
		double min_distance = 0;
		for (Trip trip : clusters.get(0))
			min_distance += trip.getLength();
		min_distance /= clusters.get(0).size();
		System.out.println("min_d " + min_distance);
		while (it.hasNext()) {
			++i;
			if (i >= limit)
				break;
			List<Trip> trips = it.next();
			Iterator<Trip> trip_it = trips.iterator();
			Trip first_trip = trip_it.next();
			System.out.println("Length:" + first_trip.getLength());
			if (first_trip.getLength() >= 1.5 * min_distance)
				break;
			while (trip_it.hasNext()) {
				if (first_trip.mergeWith(trip_it.next()))
					break;
			}
			JSONObject route = new JSONObject();
			JSONArray gps = new JSONArray();
			JSONArray polygons = new JSONArray();
			route.put("trip_id", trips.get(0).getId());
			route.put("gps_points", gps);
			route.put("rects", polygons);
			routes.put(route);
			List<LngLatAlt> exteriors = new ArrayList<LngLatAlt>();
			double v_x, v_y;
			Iterator<Map.Entry<Double, Double>> point_it = trips.get(0)
					.getTraces().iterator();
			Map.Entry<Double, Double> pre_point = point_it.next();
			Map.Entry<Double, Double> cur_point;
			while (point_it.hasNext()) {
				cur_point = point_it.next();
				if (cur_point.equals(pre_point))
					continue;
				exteriors.clear();
				double x1 = pre_point.getKey();
				double y1 = pre_point.getValue();
				double x2 = cur_point.getKey();
				double y2 = cur_point.getValue();
				pre_point = cur_point;
				double n_x = x2 - x1;
				double n_y = y2 - y1;
				double tmp = Math.sqrt(n_x * n_x + n_y * n_y);
				n_x /= tmp;
				n_y /= tmp;
				v_x = n_x;
				v_y = n_y;
				tmp = -n_x;
				n_x = n_y;
				n_y = tmp;

				exteriors.add(new LngLatAlt(x1 - v_x * 0.0001 + 0.0000101
						* extend * n_x, y1 - v_y * 0.0001 + 0.0000101 * extend
						* n_y));
				exteriors.add(new LngLatAlt(x1 - v_x * 0.0001 - 0.0000101
						* extend * n_x, y1 - v_y * 0.0001 - 0.0000101 * extend
						* n_y));
				exteriors.add(new LngLatAlt(x2 + v_x * 0.0001 - 0.0000101
						* extend * n_x, y2 + v_y * 0.0001 - 0.0000101 * extend
						* n_y));
				exteriors.add(new LngLatAlt(x2 + v_x * 0.0001 + 0.0000101
						* extend * n_x, y2 + v_y * 0.0001 + 0.0000101 * extend
						* n_y));
				Polygon polygon = new Polygon(exteriors);
				polygons.put(objectMapper.writeValueAsString(polygon));
			}

			for (Map.Entry<Double, Double> point : trips.get(0).getTraces()) {
				Point p = new Point(new LngLatAlt(point.getKey(),
						point.getValue()));
				gps.put(objectMapper.writeValueAsString(p));
			}
		}
		Calendar expires = Calendar.getInstance();
		expires.add(Calendar.YEAR, 1);
		return res;
	}
}