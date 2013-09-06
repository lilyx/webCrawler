import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.util.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class Crawler {
 Connection connection;
 int urlID;
 int NextURLID;
 int NextURLIDScanned;
 int MaxURLs = 5000;
 public Properties props;
 Queue<String> queue=new LinkedList<String>();
 
 Crawler() {
  urlID = 0;
  NextURLID = 0;
  NextURLIDScanned =0;
 }

 public void startCrawl() {
  String root="";

  try {
   readProperties();
   root = props.getProperty("crawler.root");
   createDB();
  } catch (Exception e) {
   e.printStackTrace();
  }
  while(NextURLIDScanned<MaxURLs){
   
   if(root==null){
    try{
     crawl();
    }catch(Exception e){
     
    }
    continue;
   }
   
   try{
   fetchURL(root);
   crawl();
   }catch(Exception e){
    continue;
   }
   
   root=queue.poll();
   
  }
  
  
 }

 public void crawl() throws SQLException, IOException {
  int urlIndex;
  while (NextURLIDScanned <= NextURLID) {
   // Fetch the url1 entry in urlIndex
   urlIndex = NextURLIDScanned;
   Statement stat = connection.createStatement();
   ResultSet result = stat
     .executeQuery("SELECT * FROM urls WHERE urlid LIKE '"
       + urlIndex + "'");
   URL url1;
   String s="";
   if (result.next()) {
    s = result.getString("url");
    url1 = new URL(s.replaceAll("\"",""));

   } else
    break;

   NextURLIDScanned++;
   
   if(s.indexOf(".pdf")==s.length()-5||s.indexOf(".jpg")==s.length()-5||
     s.indexOf(".mp4")==s.length()-5||s.indexOf(".doc")==s.length()-5||s.indexOf(".ppt")==s.length()-5||
     s.indexOf(".JPG")==s.length()-5||s.indexOf(".avi")==s.length()-5||s.indexOf(".ps")==s.length()-4||
     s.indexOf(".gz")==s.length()-4||s.indexOf(".mp3")==s.length()-5||s.indexOf(".docx")==s.length()-6||
     s.indexOf(".wav")==s.length()-5||s.indexOf(".wma")==s.length()-5||s.indexOf(".ps.Z")==s.length()-6||
     s.indexOf(".gif")==s.length()-5||s.indexOf(".eps")==s.length()-5||s.indexOf(".zip")==s.length()-5||
     s.indexOf(".pptx")==s.length()-6||s.indexOf(".tgz")==s.length()-5||s.indexOf(".jpeg")==s.length()-6)
   continue;
   // Get the first 100 characters or less of the document from url1
   // without tags. Add this description to the URL record in the URL
   // table.

   InputStreamReader in = new InputStreamReader(url1.openStream());
   // read contents into string builder
   StringBuilder input = new StringBuilder();
   int ch;
   while ((ch = in.read()) != -1) {
    input.append((char) ch);
   }
   in.close();

   
    
    Document doc = Jsoup.parse(url1.openStream(), "iso-8859-1",s.replaceAll("\"",""));
    String text = doc.text();
   
    //System.out.println("Old text:\n"+text);
    
    String description = text.replaceAll("\\<.*?\\>[\\s\\S]*?\\<.*?\\>", "");
    description = description.replaceAll("<!--[\\s\\S]*?-->","");
    description = description.replaceAll("\\<.*?\\>[\\s\\S]*","");
    description = description.replaceAll("&[\\s\\S]*?;","");
    description = description.replaceAll("\'","");

    

    Pattern p1 = Pattern.compile("[\\s]|[\t]|[\r]|[\n]|[^\\p{ASCII}]"); 
    Matcher m1 = p1.matcher(description);
    description=m1.replaceAll(" ");
//    System.out.println("index:"+urlIndex+" text:\n"+description.substring(0,3));
    if (description.length() >= 100){
     stat.executeUpdate("UPDATE urls SET description= '"
      + description.substring(0, 100)+ "' WHERE urlid='" + urlIndex
      + "'");
    }
    else
     stat.executeUpdate("UPDATE urls SET description= '" + description
      + "' WHERE urlid='" + urlIndex + "'");
    
   

   // For each url2 in the the links in the anchor tags of this
   // document {
   // fetch the url2 in the link
   // if it is not text/html continue;
   // if (NextURLID < MaxURLs && url2 is not already in URL table) {
   // put (NextURLID, url2) in the URL table
   // NextURLID++
   // }
   // }
   String patternString = "<a\\s+href\\s*=\\s*(\"[^\"]*\"|[^\\s>]*)\\s*>";
   Pattern pattern = Pattern.compile(patternString,
     Pattern.CASE_INSENSITIVE);
   Matcher matcher = pattern.matcher(input);

   while (matcher.find()) {
    String urlFound = matcher.group(1);
    
    if(urlFound.replaceAll("\"","").endsWith("/"))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.replaceAll("\"","").endsWith("."))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.contains("'"))
     urlFound=urlFound.replaceAll("'","");
    if(urlFound.contains(" "))
     urlFound=urlFound.replaceAll(" ","");
    
    if(!url1.getPath().equals(""))
     urlFound="\""+makeAbsoluteURL(urlFound.replaceAll("\"", ""),s.replaceAll("\"","").substring(0,s.replaceAll("\"","").indexOf(url1.getPath())))+"\"";
    else 
     urlFound="\""+makeAbsoluteURL(urlFound.replaceAll("\"", ""),s.replaceAll("\"",""))+"\"";

    if(urlFound.replaceAll("\"","").endsWith("/"))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.replaceAll("\"","").endsWith("."))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.contains("'"))
     urlFound=urlFound.replaceAll("'","");
    if(urlFound.contains(" "))
     urlFound=urlFound.replaceAll(" ","");
    
    if(!urlFound.contains("http://www.cs.purdue.edu"))
     continue;
    
    try {
     URL url2 = new URL(urlFound);
     InputStream in1 = url2.openStream();
     in1.close();
    } catch (Exception e) {
     continue;
    }

    if (NextURLID < MaxURLs && !urlInDB(urlFound)) {
     try{
      insertURLInDB(NextURLID, urlFound);
     }catch(Exception e){
      continue;
     }
     NextURLID++;
    }
   }

   // Get the document in url1 without tags
   // for each different word in the document {
   // In Word Table get the (word, URLList) for this word and append
   // urlIndex at the end of URLList
   // or create a new (word, URLList) if the entry does not exist. }
   
   if (urlIndex == 0) {
    try {
     stat.executeUpdate("DROP TABLE WORDS");
    } catch (Exception e) {
    }
    stat.executeUpdate("CREATE TABLE WORDS (word VARCHAR(200), urllist VARCHAR(50000))");
   }
   String p= "[a-zA-Z]*";
   Pattern pp=Pattern.compile(p);
   Matcher m = pp.matcher(description);
   while(m.find()){
    String words=m.group();
    if(words.equals("")) continue;
    String query="SELECT * FROM words WHERE word LIKE \""+ words+"\"";
//    System.out.println("SELECT * FROM words WHERE word LIKE \""+ words+"\"");
    ResultSet newresult = stat.executeQuery(query);
    if (!newresult.next()) {
     stat.executeUpdate("INSERT INTO words VALUES (\"" + words+ "\",\"" +""+ urlIndex + "\")");
     
    } else {
     String url=newresult.getString("urllist");
     String[] urllist=url.split(",");
     List<String> list = Arrays.asList(urllist);
     String str=Integer.toString(urlIndex);
              if (list.contains(str))
               continue;
              else
               stat.executeUpdate("UPDATE words SET urllist= \""+url+","+urlIndex+"\""+ "WHERE word=\"" + words + "\"");
    }
   }

  }// while
 }

 public void readProperties() throws IOException {
  props = new Properties();
  FileInputStream in = new FileInputStream("database.properties");
  props.load(in);
  in.close();
 }

 public void openConnection() throws SQLException, IOException {
  String drivers = props.getProperty("jdbc.drivers");
  if (drivers != null)
   System.setProperty("jdbc.drivers", drivers);

  String url = props.getProperty("jdbc.url");
  String username = props.getProperty("jdbc.username");
  String password = props.getProperty("jdbc.password");

  connection = DriverManager.getConnection(url, username, password);
 }

 public void createDB() throws SQLException, IOException {
  openConnection();

  Statement stat = connection.createStatement();

  // Delete the table first if any
  try {
   stat.executeUpdate("DROP TABLE URLS");
  } catch (Exception e) {
  }

  // Create the table
  stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(200))");

 }

 public boolean urlInDB(String urlFound) throws SQLException, IOException {
  Statement stat = connection.createStatement();
  System.out.println("SELECT * FROM urls WHERE url LIKE '" + urlFound+ "'");
  ResultSet result = stat
    .executeQuery("SELECT * FROM urls WHERE url LIKE '" + urlFound+ "'");

  if (result.next()) {
    System.out.println("URL "+urlFound+" already in DB");
   return true;
  }
   System.out.println("URL "+urlFound+" not yet in DB");
  return false;
 }

 public void insertURLInDB(int urlid, String url) throws SQLException,IOException {

  URL u = new URL(url.replaceAll("\"",""));

  InputStream in = u.openStream();
  in.close();
  System.out.println(url);
  Statement stat = connection.createStatement();
  String query = "INSERT INTO urls VALUES ('" + urlid + "','" + url+ "','')";
  // System.out.println("Executing "+query);
  stat.executeUpdate(query);
  if(!queue.contains(url.replaceAll("\"","")))
   queue.add(url.replaceAll("\"",""));
  System.out.println(url);
 }

  public String makeAbsoluteURL(String url, String parentURL) {
   if (url.contains("://")) {
    return url;
   }
   
   String newString = url;
   if (url.indexOf(parentURL.split("\\.")[1]) == -1) {
           newString = parentURL
                   + "/"
                   + (url.startsWith("/") ? url.substring(1): url);
       }
       return newString;
 
  }
 
 public void fetchURL(String urlScanned) {
  try {
   URL url = new URL(urlScanned);
   System.out.println("urlscanned=" + urlScanned + " url.path="
     + url.getPath());

   // open reader for URL
   InputStreamReader in = new InputStreamReader(url.openStream());

   // read contents into string builder
   StringBuilder input = new StringBuilder();
   int ch;
   while ((ch = in.read()) != -1) {
    input.append((char) ch);
   }
   in.close();
   // search for all occurrences of pattern
   String patternString = "<a\\s+href\\s*=\\s*(\"[^\"]*\"|[^\\s>]*)\\s*>";
   Pattern pattern = Pattern.compile(patternString,
     Pattern.CASE_INSENSITIVE);
   Matcher matcher = pattern.matcher(input);

   while (matcher.find()) {
    int start = matcher.start();
    int end = matcher.end();
    String match = input.substring(start, end);
    System.out.println(match);
    String urlFound = matcher.group(1);
    
    if(urlFound.replaceAll("\"","").endsWith("/"))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.replaceAll("\"","").endsWith("."))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.contains("'"))
     urlFound=urlFound.replaceAll("'","");
    if(urlFound.contains(" "))
     urlFound=urlFound.replaceAll(" ","");
    
    if(!url.getPath().equals(""))
     urlFound="\""+makeAbsoluteURL(urlFound.replaceAll("\"", ""),urlScanned.substring(0,urlScanned.indexOf(url.getPath())))+"\"";
    else 
     urlFound="\""+makeAbsoluteURL(urlFound.replaceAll("\"", ""),urlScanned)+"\"";

    if(urlFound.replaceAll("\"","").endsWith("/"))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.replaceAll("\"","").endsWith("."))
     urlFound=urlFound.substring(0,urlFound.length()-2)+"\"";
    if(urlFound.contains("'"))
     urlFound=urlFound.replaceAll("'","");
    if(urlFound.contains(" "))
     urlFound=urlFound.replaceAll(" ","");   

    if(!urlFound.contains("http://www.cs.purdue.edu"))
     continue;
    
//     System.out.println(urlFound);
    
    // Check if it is already in the database
    if (!urlInDB(urlFound)) {
     urlID = NextURLID;
     try{
      insertURLInDB(urlID, urlFound);
     }catch (Exception e){
      continue;
     }
     NextURLID++;
    }
   }
  } catch (Exception e) {
   e.printStackTrace();
  }
 }

 public static void main(String[] args) {
  Crawler crawler = new Crawler();
  crawler.startCrawl();
 }
}
