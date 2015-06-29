package com.taxi.api;

import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Calendar;

import javax.imageio.ImageIO;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/img")
public class GISPicService extends GISBaseService {
	@Context private UriInfo context;
	@Context private HttpServletRequest servletRequest;
	@Context private ServletConfig servletConfig;
    @Context private ServletContext servletContext;
    @Context private HttpServletResponse servletResponse;
	
	@GET
	@Path("/{z}/{x}/{y}")
	@Produces("image/png")
	public Response getMapImage(
			@PathParam("z") String p_z,
			@PathParam("x") String p_x,
			@PathParam("y") String p_y)throws Exception
	{
		String baseDir = servletConfig.getServletContext().getRealPath("/").replace("\\", "/");
		String emtpyTileImg = baseDir + "WebLib/leaflet/images/empty.png";
		File imgFile = new File(emtpyTileImg);
		
		BufferedImage image = ImageIO.read(imgFile);
		ServletOutputStream out = servletResponse.getOutputStream();
		ImageIO.write(image, "png", out);
		out.close();
		return null;
	}
}