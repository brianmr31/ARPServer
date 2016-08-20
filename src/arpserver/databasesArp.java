/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package arpserver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author brian
 */
public class databasesArp {
    private Connection connect = null;
    private List<String> dataTable = new ArrayList<String>() ;
    private boolean status ;
    private static Statement statement = null;
    private static PreparedStatement preparedStmt = null;
    private String query = null;
    private ResultSet rs ;
    public databasesArp(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            status = false ;
        }
        try {
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/alphatestV2?"
                    + "user=root&password=toor");
        } catch (SQLException ex) {
            status = false ;
        } 
        status = true ;
    }
    public boolean getStatus(){
        return status;
    }
    public void stopProc(){
        try {
            connect.close();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   public void insertSettingDefault(){
        query = "insert into setting values(1,"+180000+",'FileName',' ','FileHadoop',' ','127.0.0.1',"+4000+");";
        //System.out.println(query);
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            preparedStmt.execute();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   public String selectDataExists(String Input){
        String x =null ;
        query = "select filename from data where filename = '"+Input+"'";
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            rs = preparedStmt.executeQuery();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            while (rs.next()){
                x = rs.getString("id");
                
                /*
                System.out.println("id         "+value[0]);
                System.out.println("intel      "+value[1]);
                System.out.println("filename   "+value[2]);
                System.out.println("pathname   "+value[3]);
                System.out.println("filehadoop "+value[4]);
                System.out.println("pathhadoop "+value[5]);
                System.out.println("ip         "+value[6]);
                System.out.println("port       "+value[7]);
                */
            } } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        return x;
        
    }
   public void insertSetting(String aa,String ab,String ac,String ad,String ae,String af,String ag){
        query = "insert into setting values(1,"+aa+",'"+ab+"','"+ac+"','"+ad+"','"+ae+"','"+af+"',"+ag+");";
        System.out.println(query);
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            preparedStmt.execute();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   public String[] selectSetting(){
       String [] value = new String[8];
        query = "select * from setting ;";
        //System.out.println(query);
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            rs = preparedStmt.executeQuery();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            while (rs.next()){
                value[0] = rs.getString("id");
                value[1] = rs.getString("intel");
                value[2] = rs.getString("filename");
                value[3] = rs.getString("pathname");
                value[4] = rs.getString("filehadoop");
                value[5] = rs.getString("pathhadoop");
                value[6] = rs.getString("ip");
                value[7] = rs.getString("port");
                
                /*
                System.out.println("id         "+value[0]);
                System.out.println("intel      "+value[1]);
                System.out.println("filename   "+value[2]);
                System.out.println("pathname   "+value[3]);
                System.out.println("filehadoop "+value[4]);
                System.out.println("pathhadoop "+value[5]);
                System.out.println("ip         "+value[6]);
                System.out.println("port       "+value[7]);
                */
            } } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        return value;
    }
    public int getidSetting(){
        int id = 0 ;
        query = "select id from setting ;";
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
            return 1 ;
        }
        try {
            rs = preparedStmt.executeQuery();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
            return 1 ;
        }
        try {
            while (rs.next()){
                id= rs.getInt("id");
                //System.out.println("id         "+id);
            } } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
            return 1 ;
        }
        return id;
    }
    public void setSetting(String aa,String ab,String ac,String ad,String ae,String af,String ag){
        query = "UPDATE setting set intel = '"+aa+"',filename = '"+ab+"',pathname= '"+ac+"'"
                    + ",filehadoop = '"+ad+"',pathhadoop = '"+ae+"' ,ip = '"+af+"',port = '"+ag+"'where id=1;";
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            preparedStmt.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void insertData(String name, String path, String timeData){
        query = "insert into data values(NULL,'"+name+"','"+path+"','"+timeData+"');";
        System.out.println(query);
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            preparedStmt.execute();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public List selectData(){
        String[] value = new String[4];
        query = "select * from data Order by id desc;";
        System.out.println(query);
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            rs = preparedStmt.executeQuery();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            while (rs.next()){
                value[0] = rs.getString("id");
                value[1] = rs.getString("filename");
                value[2] = rs.getString("pathname");
                value[3] = rs.getString("time");
                dataTable.add(0,value[0]);
                dataTable.add(1,value[1]);
                dataTable.add(2,value[2]);
                dataTable.add(3,value[3]);
                /*
                System.out.println("id         "+value[0]);
                System.out.println("filename   "+value[1]);
                System.out.println("pathname   "+value[2]);
                System.out.println("pathname   "+value[3]);
                */
            } } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        return dataTable;
    }
    public int getMaxData(){
        int i = 0 ;
        query = "select max(id) from data ;";
        //System.out.println(query);
        try {
            preparedStmt = connect.prepareStatement(query);
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            rs = preparedStmt.executeQuery();
        } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            while (rs.next()){
                i = rs.getInt("max(id)");
                //System.out.println("id         "+id);
            } } catch (SQLException ex) {
            Logger.getLogger(databasesArp.class.getName()).log(Level.SEVERE, null, ex);
        }
        return i ;
    }
}
