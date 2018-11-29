package edu.zju.gis.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by admin on 2018/3/17.
 */
public class DataServlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String level = request.getParameter("level");
        String path = null;
        if(level.equals("s")){
            path = "H:/MyGIS/geojson/XZQ.json";
        }else if(level.equals("b")){
            path = "H:/MyGIS/geojson/TDGHDL.json";
        }
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line = null;
        StringBuilder sb = new StringBuilder();
        while((line = br.readLine())!=null){
            sb.append(line);
        }
        response.setCharacterEncoding("UTF-8");
        PrintWriter pw = response.getWriter();
        pw.write(sb.toString());
        System.out.println("±ª∑√Œ ¡À");
    }
}
