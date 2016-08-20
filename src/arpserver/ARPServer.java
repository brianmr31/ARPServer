/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package arpserver;

/**
 *
 * @author brian
 */
public class ARPServer {
    private static Setting setting ;
    private static serverUdp udp ;
    private static databasesArp db ;
    private static Interface interfacex ;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        db = new databasesArp();
        System.out.println(db.getidSetting());
        db.selectSetting();
        if(db.getidSetting() == 0){
             setting = new Setting(db);
             setting.show();
        }
        udp = new serverUdp(db);
        interfacex = new Interface(db,udp);
        udp.setinterfaceX(interfacex);
        interfacex.show();
        udp.start();
        //db.selectSttting();
        //db.setSetting(18000, "1", "1", " ", " ", " ", 4000);
        //db.selectSttting();
        //db.insertData("", "", "");
        //db.insertData("", "", "");
        //db.insertData("", "", "");
        //System.out.println(db.selectSetting());
    }
    
}
