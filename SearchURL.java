package search;

import java.io.*;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;


/**
 * Servlet implementation class SearchURL
 */
@WebServlet("/SearchURL")
public class SearchURL extends HttpServlet {
 private static final long serialVersionUID = 1L;
 Connection connection;
 public Properties props;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public SearchURL() {
        super();
        // TODO Auto-generated constructor stub
    }

 /**
  * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
  */
 protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
  response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println("<html>");
        out.println("<head>");
        out.println("<title>Search</title>");
        out.println("</head>");
        out.println("<body>");
        out.println("<CENTER>");
        out.println("<h3>Find Related URL</h3>");
        out.println("keyword in this request:<br>");
        out.println("</CENTER>");
        String keyword = request.getParameter("keyword");
        
        if (keyword != null) {
         out.println("<CENTER>");
            out.println("key:");
            out.println(HTMLFilter.filter(keyword) +
                         "<br>");
            out.println("</CENTER>");
           try{
            readProperties();
            openConnection();
            Statement stat = connection.createStatement();
            ResultSet result = null;
            String kw[]=keyword.split(" ");
            List<String> urls=new ArrayList<String>();
            List<String> tempUrl=new ArrayList<String>();
            for(int n=0;n<kw.length;n++){
             result = stat
                .executeQuery("SELECT * FROM words WHERE word LIKE '"
                  + kw[n] + "'");
             if(result.next()){
//              out.println(HTMLFilter.filter(result.getString("urllist")) +
//                               "<br>");
              String u[]=result.getString("urllist").split(",");
              if(kw.length>1){
               for(int j=0;j<u.length;j++){
                if(tempUrl.contains(u[j]))
                 urls.add(u[j]);
                else
                 tempUrl.add(u[j]);
               }
              }
              else{
               for(int j=0;j<u.length;j++){
                urls.add(u[j]);
                }
               }
              }
             }
           
            ResultSet res;
            int num=1;
            for(String i:urls){
              res = stat
              .executeQuery("SELECT * FROM urls WHERE urlid LIKE '"
                 + i + "'");
             if(res.next()){
              String url=res.getString("url");
              url=url.replaceAll("\"", "");
//               <tr><td><a href="/myapp/mypage.jsp?id=${id}">Link ${id}</a></td></tr>
              out.println(""+num+". "+"<b><tr><td><a href=" +HTMLFilter.filter(url)+ ">"+HTMLFilter.filter(url) +"</a></td></tr></b>"+"<br>");
              String description=res.getString("description");
              num++;
              out.println(description+"<br>");
//              out.println();
             }
            }
//            out.println("Done<br>");
           }catch(Exception e){
            e.printStackTrace();
           }

        } else {
            out.println("<CENTER>");
            out.println("No Parameters, Please enter some");
            out.println("</CENTER>");
        }

        out.println("<P>");
        out.print("<form action=\"");
        out.print("SearchURL\" ");
        out.println("method=POST>");
        out.println("<CENTER>");
        out.println("Find:");
        out.println("<input type=text size=20 name=keyword>");
        out.println("<br>");
        out.println("<input type=submit value=\"Search\">");
        out.println("</CENTER>");
        out.println("</form>");
        out.println("</body>");
        out.println("</html>");
 }

 /**
  * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
  */
 protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
  doGet(request, response);
 }
 
 public void readProperties() throws IOException {
  props = new Properties();
  FileInputStream in = new FileInputStream("/Users/xx/Dropbox/cs390-java/database.properties");
  props.load(in);
  in.close();
 }

 public void openConnection() throws SQLException, IOException, ClassNotFoundException {
  String drivers = props.getProperty("jdbc.drivers");
  if (drivers != null)
   System.setProperty("jdbc.drivers", drivers);

  String url = props.getProperty("jdbc.url");
  String username = props.getProperty("jdbc.username");
  String password = props.getProperty("jdbc.password");
  Class.forName("com.mysql.jdbc.Driver");
  connection = DriverManager.getConnection(url, username, password);
 }


}
