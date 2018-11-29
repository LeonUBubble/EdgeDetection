package edu.zju.gis.servlet;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by admin on 2018/3/17.
 */
public class AjaxServlet extends javax.servlet.http.HttpServlet {
    protected void doPost(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws javax.servlet.ServletException, IOException {
        String name = request.getParameter("name");
        String url = request.getParameter("url");
        String a = request.getParameter("haha");
        response.setCharacterEncoding("UTF-8");
        PrintWriter pw = response.getWriter();
        pw.write("name:"+name+"\nurl:"+url+"\nhaha:"+a);
        System.out.println("被访问了-post");
    }

    protected void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws javax.servlet.ServletException, IOException {
        PrintWriter pw = response.getWriter();
        pw.write("XXX");
        System.out.println("被访问了-get");
    }
}
