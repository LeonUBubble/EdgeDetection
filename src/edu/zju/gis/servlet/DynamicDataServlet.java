package edu.zju.gis.servlet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.zju.gis.bean.Point;
import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.cache.Cache;
import edu.zju.gis.utils.RequestDealUtils;

/**
 * Servlet implementation class DynamicDataServlet
 */

public class DynamicDataServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DynamicDataServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		long start = System.currentTimeMillis();
		double p1x = Double.parseDouble(request.getParameter("p1x"));
		System.out.println(p1x);
		double p1y = Double.parseDouble(request.getParameter("p1y"));
		System.out.println(p1y);
		double p2x = Double.parseDouble(request.getParameter("p2x"));
		System.out.println(p2x);
		double p2y = Double.parseDouble(request.getParameter("p2y"));
		System.out.println(p2y);
		
		Point p1 = new Point(p1x, p1y);
		Point p2 = new Point(p2x, p2y);
		Rectangle extent = new Rectangle(p1,p2);
		String geojson = null;
		try {
			geojson = RequestDealUtils.getDataFromHBase(extent);
		} catch (Exception e) {
			e.printStackTrace();
		}
		response.setContentType("application/json;charset=utf-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter pw = response.getWriter();
        pw.write(geojson);
        System.out.println("±ª∑√Œ ¡À");
        System.out.println(System.currentTimeMillis()-start);
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
