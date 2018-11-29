package edu.zju.gis.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.zju.gis.utils.RequestDealUtils;

/**
 * Servlet implementation class DynamicDataServlet
 */

public class DynamicDataServlet2 extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DynamicDataServlet2() {
        super();
    }

	/**
	 * 获取浙江省的行政区数据返回到前端
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		long start = System.currentTimeMillis();
		String geojson = null;
		try {
			geojson = RequestDealUtils.getXzqDataFromHBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
		response.setHeader("Content-type","application/json;charset=utf-8");
		response.setContentType("application/json;charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter pw = response.getWriter();
        pw.write(geojson);
//        System.out.println(geojson);
        System.out.println("被访问了haha");
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
