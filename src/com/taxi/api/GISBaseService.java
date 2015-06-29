package com.taxi.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.codehaus.jettison.json.JSONObject;
import org.geojson.GeoJsonObject;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.Point;

import com.db.DBUtil;
import com.gis.model.GeometryType;
import com.taxi.base.BaseService;

public class GISBaseService extends BaseService {
	public static void closeResultSetResource(ResultSet rs)
	{
		Statement stmt = null;
		Connection conn = null;
		try {
			try {
				stmt = rs.getStatement();
				conn = stmt.getConnection();
			} finally {
				rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			}
		} catch (NullPointerException e) {
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static void closeStatementResource(Statement st)
	{
		Connection conn = null;
		try {
			try {
				conn = st.getConnection();
			} finally {
				st.close();
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
		}
	}

	public GeoJsonObject getGeometry(GeometryType type, ResultSet rs)
	{
		double scale = 10000000;
		try {
			switch (type) {
			case POINT:
				Point point = null;
				if (rs.next())
					point = new Point(rs.getDouble("longitude"),
							rs.getDouble("latitude"));
				return point;
			case LINE_STRING:
				LineString line_string = new LineString();
				while (rs.next()) {
					line_string.add(new LngLatAlt(rs.getDouble("longitude"), rs
							.getDouble("latitude")));
				}
				return line_string;
			case MULTI_LINE_STRING:
				break;
			case MULTI_POINT:
				break;
			case MULTI_POLYGON:
				break;
			case POLYGON:
				break;
			default:
				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public JSONObject getNearestSectionPoint(double lng, double lat)
	{
		JSONObject node = new JSONObject();
		try {
			PreparedStatement stmt_vertex_id = DBUtil.getInstance().createSqlStatement("select "
					+ "taxi.get_nearest_vertex(?,?)", lng, lat);
			ResultSet rs_vertex = stmt_vertex_id.executeQuery();
			long id = -1;
			if (rs_vertex.next())
				id = rs_vertex.getLong(1);
			rs_vertex.close();
			PreparedStatement stmt_vertex = stmt_vertex_id.getConnection()
					.prepareStatement(
							"select st_x(the_geom) x," + "st_y(the_geom) y "
									+ "from taxi.edges_vertices_pgr "
									+ "where id = ?");
			stmt_vertex_id.close();
			stmt_vertex.setObject(1, id);
			rs_vertex = stmt_vertex.executeQuery();
			if (rs_vertex.next()) {
				node.put("node_id", id);
				System.out.println(id);
				node.put("x", rs_vertex.getDouble("x"));
				node.put("y", rs_vertex.getDouble("y"));
			}
			rs_vertex.close();
			closeStatementResource(stmt_vertex);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return node;
	}
}