package edu.zju.gis.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.zju.gis.service.StaticJsonData;

/**
 * Servlet implementation class GetGHQStaticData
 */

public class GetGHQStaticData extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetGHQStaticData() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		response.setHeader("Content-type","application/json;charset=utf-8");
		response.setContentType("application/json;charset=UTF-8");
        response.setCharacterEncoding("UTF-8");     
        String type = request.getParameter("type");
        System.out.println("type:"+type);
        String json = null;
        if("".equals(type)||null == type) {
        	json = StaticJsonData.getDataById("1");
        }else {
        	json = StaticJsonData.getDataByIdAndType("1", type);
        }
		PrintWriter pw = response.getWriter();
		System.out.println(json);
		pw.write(json);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
