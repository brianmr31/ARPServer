/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package arpserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author brian
 */
public class serverUdp extends Thread{
    private static String[] io = new String[2];
    private static String Strinput = null ;
    private static String input = null ;
    private static Object[] testO = new Object[4] ;
    private static String hadooppath,hdfspath,ip,name,path,interval,timeData;
    private static int port,last;
    private static boolean statusinput,statusdb ;
    private databasesArp db;
    private static String[] value = new String[7] ;
    private DatagramSocket serverSocket  ;
    private DatagramPacket receivePacket;
    private byte[] receiveData ;
    private static byte[] sendData;
    private ReentrantLock lock;
    private Lock mutex = new ReentrantLock(true);
    private static FileWriter fw= null;
    private static File filex =null;
    private static File filecheck ;
    private String sentence = null ;
    private static long time ;
    private static long timeI ;
    private static Thread cf ;
    private static boolean inputData =false ;
    private static SimpleDateFormat dateFormat ;
    private Interface x ;
    private static Path inFile = null;
    private static Path outFile = null;
    private static FileSystem fs = null;
    private static Configuration conf = new Configuration();
    public Process p ;
    private BufferedReader stdInput ;
    private BufferedReader stdError ;
    private String s = null;
    public serverUdp(databasesArp db){
        this.db = db ;
        value =this.db.selectSetting();
        interval = value[1] ;
        name = value[2] ;
        path = value[3] ;
        hadooppath = value[4] ;
        hdfspath = value[5] ;
        ip = value[6];
        port = Integer.parseInt(value[7]);
        last = this.db.getMaxData();
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        startHadoop();
        try {             
            serverSocket = new DatagramSocket(port);
        } catch (SocketException ex) {
            Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            fs = FileSystem.get(conf);
        } catch (IOException ex) {
            Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
        }
        receiveData = new byte[150];
        sendData = new byte[150];
        lock = new ReentrantLock();
        cf = new Thread(new Runnable() {

            @Override
            public void run() {
                    getTimeI();
                    getTime();
                    while(true){
                         //System.out.println("++++"+time +" "+ timeI);
                         if(time > timeI ){
                             System.out.println("++++"+time +" "+ timeI);
                             //try {
                             //    fw.close();
                             //} catch (IOException ex) {
                             //    Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
                             //}
                            last++;
                            getTimeI();
                            getTime();
                            System.out.println("save");
                            inputData = true ;
                            x.setNetwork(statusinput);
                            x.setDataBases(db.getStatus());
                            createFile();
                         }
                         getTime();
                    }
                
             }
        });
    }
    public void setinterfaceX(Interface Interfacex){
        this.x = Interfacex;
    }
    public static void getTime(){
        synchronized (serverUdp.class) {
            time = System.currentTimeMillis();
        }
    }
    public static void getTimeI(){
        synchronized (serverUdp.class) {
            time = System.currentTimeMillis();
            timeI = time + Long.parseLong(interval);
        }
    }
    public String getPath(){
        return this.path;
    }
    public String getNamepath(){
        return this.name;
    }
    public String getIp(){
        return this.ip;
    }
    public int getPort(){
        return this.port;
    }
    public String getHdfspath(){
        return this.hdfspath;
    }
    public String getHadooppath(){
        return this.hadooppath;
    }
    public void setHdfspath(String hdfspath){
        this.hdfspath = hdfspath ;
    }
    public void setHadooppath(String hadooppath){
        this.hdfspath = hadooppath ;
    }
    public boolean getStatusInput(){
        return statusinput;
    }
    public boolean getStatusDb(){
        return statusdb;
    }
    public String getDataName(){
        return this.name+""+this.last ;
    }
    public String getDataPath(){
        return this.path ;
    }
    public String getDataTime(){
        return this.timeData;
    }
    public void stopProc(){
        try {
            fw.close();
            serverSocket.close();
        } catch (IOException ex) {
            Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void startHadoop(){
        try {
            p = Runtime.getRuntime().exec(value[4]+"/sbin/start-all.sh");
            stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
           
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }
        } catch (IOException ex) {
            Logger.getLogger(Interface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void updateSetting(){
        value = this.db.selectSetting();
        interval = value[0] ;
        name = value[1] ;
        path = value[2] ;
        hadooppath = value[3] ;
        hdfspath = value[4] ;
        ip = value[5];
    }
    public void createFile(){
        System.out.println((this.path+"/"+this.name+""+this.last));
        String check = this.db.selectDataExists(this.name+""+this.last) ;
        filex=new File(this.path+"/"+this.name+""+this.last+".txt");
        this.timeData = dateFormat.format(new Date());
        System.out.println("Check File Exists : "+filex.exists());
        if(!filex.exists()) {
           try {
                filex.createNewFile();
            } catch (IOException ex) {
                Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if(check==null){
            this.db.insertData(this.path, this.name+""+this.last, this.timeData);
            testO[0] = this.db.getMaxData();
            testO[1] = getDataPath() ;
            testO[2] = getDataName();
            testO[3] = getDataTime();
            x.model.addRow(testO);
        }
        
        try {
            fw = new FileWriter(filex);
        } catch (IOException ex) {
            Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println(this.hadooppath+" "+this.hdfspath);
        processHadoop(this.name+""+(this.last-1));
        
    }

    public void uploadFile(String inputLocal, String inputServer){
        inFile = new Path(inputLocal);
        outFile = new Path(inputServer);
       
        try {
            //fs.create(inFile);
            if (!fs.exists(inFile)){
                fs.copyFromLocalFile(inFile, outFile);
                System.out.println("++++"+inputLocal+"->"+inputServer);
            }
        } catch (IOException ex) {
            Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void downloadFile(String outputLocal, String outputServer){
        inFile = new Path(outputLocal+"/part-r-00000");
        outFile = new Path(outputServer+"/test.txt");
        try {
            if(fs.exists(inFile)){
                inFile = new Path(outputLocal+"/part-r-00000");
                outFile = new Path("/var/www/html/SI-ARP/data/TestTAFinal/"+outputServer+"/Q.txt");
                fs.moveToLocalFile(inFile, outFile);
                inFile = new Path(outputLocal+"ArpScn/part-r-00000");
                outFile = new Path("/var/www/html/SI-ARP/data/TestTAFinal/"+outputServer+"/ArpScn.txt");
                fs.moveToLocalFile(inFile, outFile);
                inFile = new Path(outputLocal+"Mitm/part-r-00000");
                outFile = new Path("/var/www/html/SI-ARP/data/TestTAFinal/"+outputServer+"/Mitm.txt");
                fs.moveToLocalFile(inFile, outFile);
                inFile = new Path(outputLocal+"Src/part-r-00000");
                outFile = new Path("/var/www/html/SI-ARP/data/TestTAFinal/"+outputServer+"/Src.txt");
                fs.moveToLocalFile(inFile, outFile);
                inFile = new Path(outputLocal+"s/part-r-00000");
                outFile = new Path("/var/www/html/SI-ARP/data/TestTAFinal/"+outputServer+"/s.txt");
                fs.moveToLocalFile(inFile, outFile);
                inFile = new Path(outputLocal+"d/part-r-00000");
                outFile = new Path("/var/www/html/SI-ARP/data/TestTAFinal/"+outputServer+"/d.txt");
                fs.moveToLocalFile(inFile, outFile);
                inFile = new Path(outputLocal+"t/part-r-00000");
                outFile = new Path("/var/www/html/SI-ARP/data/TestTAFinal/"+outputServer+"/t.txt");
                fs.moveToLocalFile(inFile, outFile);
            }
        } catch (IOException ex) {
            Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void processHadoop(String inputfile){
        input = this.hadooppath+"/bin/hadoop fs -copyFromLocal "+this.path+"/"+inputfile+".txt "+this.hdfspath+"/"+inputfile+".txt" ;
        io[0] = this.hdfspath+"/"+inputfile+".txt";
        io[1] = this.hdfspath+"/"+inputfile+"output";
        filecheck = new File(this.path+"/"+inputfile+".txt");
        System.out.println("====================================================");
        System.out.println(io[0]);
        System.out.println(io[1]);
        System.out.println("====================================================");
        if(filecheck.exists()){
            uploadFile(this.path+"/"+inputfile+".txt",io[0]);
            try {
                int res = ToolRunner.run(new Configuration(), new HadoopTool(), io);
            } catch (Exception ex) {
                Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
            }
            filecheck = new File(this.hdfspath+"/"+inputfile);
            if(!filecheck.exists()){
                filecheck.mkdir();
            }
            downloadFile(io[1],"/"+inputfile);
            //try {
            //    int res = ToolRunner.run(new Configuration(), new hadoopTool(this.c), io);
            //} catch (Exception ex) {
            //    Logger.getLogger(serverUdp.class.getName()).log(Level.SEVERE, null, ex);
            //}
        }else{
            System.out.println("File Tidak ada");
        }
        
    }
    public void run() {
        createFile();
        cf.start();
        while(true){
            mutex.lock();
            try {
                receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                receiveData = new byte[receiveData.length];
                sentence = new String( receivePacket.getData());
                if(sentence != null){
                    statusinput = true ;
                    System.out.println(sentence);
                    fw.write(sentence.replace(";", " ").replace("\00", "")+"\n");
                    fw.flush();
                }else{
                    statusinput = false ;
                }
            } catch (IOException ex) {
                
            }
            mutex.unlock();
               
        }
    }
}
