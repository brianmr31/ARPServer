/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package arpserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author brian
 */
public class HadoopTool extends Configured implements Tool {
    public static class DetectMitm implements WritableComparable<DetectMitm>{
        private Text time ;
        private Text ArpMacSrc ;
        private Text ArpMacDst ;
        private Text ArpIpSrc;
        private Text ArpIpDst;
        private Text status;
        public DetectMitm(){
            this.time = new Text();
            this.ArpMacSrc = new Text();
            this.ArpMacDst = new Text();
            this.ArpIpSrc = new Text();
            this.ArpIpDst = new Text();
            this.status = new Text();
        }
        public DetectMitm(Text time,Text ArpMacSrc,Text ArpMacDst,Text ArpIpSrc,Text ArpIpDst,Text status){
            this.time = time;
            this.ArpMacSrc = ArpMacSrc;
            this.ArpMacDst = ArpMacDst;
            this.ArpIpSrc = ArpIpSrc;
            this.ArpIpDst = ArpIpDst;
            this.status = status;
        }
        public void set(Text time,Text ArpMacSrc,Text ArpMacDst,Text ArpIpSrc,Text ArpIpDst,Text status){
            this.time = time;
            this.ArpMacSrc = ArpMacSrc;
            this.ArpMacDst = ArpMacDst;
            this.ArpIpSrc = ArpIpSrc;
            this.ArpIpDst = ArpIpDst;
            this.status = status;
        }
        public Text getTime(){
            return this.time;
        }
        public Text getArpMacSrc(){
            return this.ArpMacSrc;
        }
        public Text getArpMacDst(){
            return this.ArpMacDst;
        }
        public Text getArpIpSrc(){
            return this.ArpIpSrc;
        }
        public Text getArpIpDst(){
            return this.ArpIpDst;
        }
        public Text getstatus(){
            return this.status;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            time.write(out);
            ArpMacSrc.write(out);
            ArpMacDst.write(out);
            ArpIpSrc.write(out);
            ArpIpDst.write(out);
            status.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            time.readFields(in);
            ArpMacSrc.readFields(in);
            ArpMacDst.readFields(in);
            ArpIpSrc.readFields(in);
            ArpIpDst.readFields(in);
            status.readFields(in);
        }

        @Override
        public int compareTo(DetectMitm o) {
            int ctime = time.compareTo(o.time);
            if(ctime != 0){
                return ctime;
            }else{
                ctime = ArpIpSrc.compareTo(o.ArpIpSrc);
                if(ctime != 0){
                    return ctime;
                }else{
                    ctime = ArpMacDst.compareTo(o.ArpMacDst);
                    if(ctime != 0){
                        return ctime;
                    }else{
                        ctime = ArpIpSrc.compareTo(o.ArpIpSrc);
                        if(ctime != 0){
                            return ctime;
                        }else{
                            ctime = ArpIpDst.compareTo(o.ArpIpDst);
                            if(ctime != 0){
                                return ctime;
                            }else{
                                ctime = status.compareTo(o.status);
                                if(ctime != 0){
                                    return ctime;
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
    }
    public static class DetectArpScan implements WritableComparable<DetectArpScan>{
        private Text time ;
        private Text MacSrc ;
        private Text MacDst ;
        private Text ArpMacSrc ;
        private Text ArpMacDst ;
        private Text ArpIpSrc;
        private Text ArpIpDst;
        private Text status;
        public DetectArpScan(){
            this.time = new Text();
            this.MacSrc = new Text();
            this.MacDst = new Text();
            this.ArpMacSrc = new Text();
            this.ArpMacDst = new Text();
            this.ArpIpSrc = new Text();
            this.ArpIpDst = new Text();
            this.status = new Text();
        }
        public DetectArpScan(Text time,Text MacSrc,Text MacDst,Text ArpMacSrc,Text ArpMacDst,Text ArpIpSrc,Text ArpIpDst,Text status){
            this.time = time;
            this.MacSrc = MacSrc;
            this.MacDst = MacDst;
            this.ArpMacSrc = ArpMacSrc;
            this.ArpMacDst = ArpMacDst;
            this.ArpIpSrc = ArpIpSrc;
            this.ArpIpDst = ArpIpDst;
            this.status = status;
        }
        public void set(Text time,Text MacSrc,Text MacDst,Text ArpMacSrc,Text ArpMacDst,Text ArpIpSrc,Text ArpIpDst,Text status){
            this.time = time;
            this.MacSrc = MacSrc;
            this.MacDst = MacDst;
            this.ArpMacSrc = ArpMacSrc;
            this.ArpMacDst = ArpMacDst;
            this.ArpIpSrc = ArpIpSrc;
            this.ArpIpDst = ArpIpDst;
            this.status = status;
        }
        public Text getTime(){
            return this.time;
        }
        public Text getMacSrc(){
            return this.MacSrc;
        }
        public Text getMacDst(){
            return this.MacDst;
        }
        public Text getArpMacSrc(){
            return this.ArpMacSrc;
        }
        public Text getArpMacDst(){
            return this.ArpMacDst;
        }
        public Text getArpIpSrc(){
            return this.ArpIpSrc;
        }
        public Text getArpIpDst(){
            return this.ArpIpDst;
        }
        public Text getstatus(){
            return this.status;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            time.write(out);
            MacSrc.write(out);
            MacDst.write(out);
            ArpMacSrc.write(out);
            ArpMacDst.write(out);
            ArpIpSrc.write(out);
            ArpIpDst.write(out);
            status.write(out);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            time.readFields(in);
            MacSrc.readFields(in);
            MacDst.readFields(in);
            ArpMacSrc.readFields(in);
            ArpMacDst.readFields(in);
            ArpIpSrc.readFields(in);
            ArpIpDst.readFields(in);
            status.readFields(in);
        }

        @Override
        public int compareTo(DetectArpScan o) {
            int ctime = time.compareTo(o.time);
            if(ctime != 0){
                return ctime;
            }else{
                ctime = MacSrc.compareTo(o.MacSrc);
                if(ctime != 0){
                    return ctime;
                }else{
                    ctime = MacDst.compareTo(o.MacDst);
                    if(ctime != 0){
                        return ctime;
                    }else{
                        ctime = ArpIpSrc.compareTo(o.ArpIpSrc);
                        if(ctime != 0){
                            return ctime;
                        }else{
                            ctime = ArpMacDst.compareTo(o.ArpMacDst);
                            if(ctime != 0){
                                return ctime;
                            }else{
                                ctime = ArpIpSrc.compareTo(o.ArpIpSrc);
                                if(ctime != 0){
                                    return ctime;
                                }else{
                                    ctime = ArpIpDst.compareTo(o.ArpIpDst);
                                    if(ctime != 0){
                                        return ctime;
                                    }else{
                                        ctime = status.compareTo(o.status);
                                        if(ctime != 0){
                                            return ctime;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
    }
    public static class DetectSrc implements WritableComparable<DetectSrc>{
        private Text time ;
        private Text MacSrc ;
        private Text ArpMacSrc ;
        private Text ArpIpSrc;
        private Text status;
        public DetectSrc(){
            this.time = new Text();
            this.MacSrc = new Text();
            this.ArpMacSrc = new Text();
            this.ArpIpSrc = new Text();
            this.status = new Text();
        }
        public DetectSrc(Text time,Text MacSrc,Text ArpMacSrc,Text ArpIpSrc,Text status){
            this.time = time;
            this.MacSrc = MacSrc;
            this.ArpMacSrc = ArpMacSrc;
            this.ArpIpSrc = ArpIpSrc;
            this.status = status;
        }
        public void set(Text time,Text MacSrc,Text ArpMacSrc,Text ArpIpSrc,Text status){
            this.time = time;
            this.MacSrc = MacSrc;
            this.ArpMacSrc = ArpMacSrc;
            this.ArpIpSrc = ArpIpSrc;
            this.status = status;
        }
        public Text getTime(){
            return this.time;
        }
        public Text getMacSrc(){
            return this.MacSrc;
        }
        public Text getArpMacSrc(){
            return this.ArpMacSrc;
        }
        public Text getArpIpSrc(){
            return this.ArpIpSrc;
        }
        public Text getstatus(){
            return this.status;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            time.write(out);
            MacSrc.write(out);
            ArpMacSrc.write(out);
            ArpIpSrc.write(out);
            status.write(out);
        }
       @Override
        public void readFields(DataInput in) throws IOException {
            time.readFields(in);
            MacSrc.readFields(in);
            ArpMacSrc.readFields(in);
            ArpIpSrc.readFields(in);
            status.readFields(in);
        }
        @Override
        public int compareTo(DetectSrc o) {
            int ctime = time.compareTo(o.time);
            if(ctime != 0){
                return ctime;
            }else{
                ctime = MacSrc.compareTo(o.MacSrc);
                if(ctime != 0){
                    return ctime;
                }else{
                    ctime = ArpMacSrc.compareTo(o.ArpMacSrc);
                    if(ctime != 0){
                        return ctime;
                    }else{
                        ctime = ArpIpSrc.compareTo(o.ArpIpSrc);
                        if(ctime != 0){
                            return ctime;
                        }else{
                            ctime = status.compareTo(o.status);
                            if(ctime != 0){
                                return ctime;
                            }
                        }
                    }
                }
            }
            return 0;
        }
    }
    public static class QuickDetect implements WritableComparable<QuickDetect>{
        private Text time ;
        private Text MacSrc ;
        private Text MacDst ;
        public QuickDetect(){
            this.time = new Text();
            this.MacSrc = new Text();
            this.MacDst = new Text();
        }
        public QuickDetect(Text time,Text MacSrc,Text MacDst){
            this.time = time;
            this.MacSrc = MacSrc;
            this.MacDst = MacDst;
        }
        public void set(Text time,Text MacSrc,Text MacDst){
            this.time = time;
            this.MacSrc = MacSrc;
            this.MacDst = MacDst;
        }
        public Text getTime(){
            return this.time;
        }
        public Text getMacSrc(){
            return this.MacSrc;
        }
        public Text getMacDst(){
            return this.MacDst;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            time.write(out);
            MacSrc.write(out);
            MacDst.write(out);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            time.readFields(in);
            MacSrc.readFields(in);
            MacDst.readFields(in);
        }
        @Override
        public int compareTo(QuickDetect o) {
            int ctime = time.compareTo(o.time);
            if(ctime != 0){
                return ctime;
            }else{
                ctime = MacSrc.compareTo(o.MacSrc);
                if(ctime != 0){
                    return ctime;
                }else{
                    ctime = MacDst.compareTo(o.MacDst);
                    if(ctime != 0){
                        return ctime;
                    }
                }
            }
            return 0;
        }
    }
public static class SMac implements WritableComparable<SMac>{
        private Text MacSrc ;
        public SMac(){
            this.MacSrc = new Text();
        } 
        public SMac(Text MacSrc){
            this.MacSrc = MacSrc;
        } 
        public void set(Text MacSrc){
            this.MacSrc = MacSrc;
        } 
        public Text getMacSrc(){
            return this.MacSrc;
        }
        
        @Override
        public void write(DataOutput d) throws IOException {
            MacSrc.write(d);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            MacSrc.readFields(di);
        }

        @Override
        public int compareTo(SMac t) {
            int ctime = MacSrc.compareTo(t.MacSrc);
            if(ctime != 0){
                return ctime;
            }
            return 0 ;
        }
     
}
public static class DMac implements WritableComparable<DMac>{
        private Text MacDst ;
        public DMac(){
            this.MacDst = new Text();
        } 
        public DMac(Text MacDst){
            this.MacDst = MacDst;
        } 
        public void set(Text MacDst){
            this.MacDst = MacDst;
        } 
        public Text getMacDst(){
            return this.MacDst;
        }
        @Override
        public void write(DataOutput d) throws IOException {
            MacDst.write(d);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            MacDst.readFields(di);
        }

        @Override
        public int compareTo(DMac t) {
            int ctime = MacDst.compareTo(t.MacDst);
            if(ctime != 0){
                return ctime;
            }
            return 0 ;
        }
     
}
public static class timeMac implements WritableComparable<timeMac>{
        private Text time ;
        public timeMac(){
            this.time = new Text();
        } 
        public timeMac(Text time){
            this.time = time;
        } 
        public void set(Text time){
            this.time = time;
        } 
        public Text gettime(){
            return this.time;
        }
        @Override
        public void write(DataOutput d) throws IOException {
            time.write(d);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            time.readFields(di);
        }

        @Override
        public int compareTo(timeMac t) {
            int ctime = time.compareTo(t.time);
            if(ctime != 0){
                return ctime;
            }
            return 0 ;
        }
     
}
public static class Smapper extends Mapper <LongWritable, Text, SMac, IntWritable>
 {
  private SMac wLog = new SMac();
  private static final IntWritable one = new IntWritable(1);
  private Text MacSrc = new Text();
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split(" ") ;
    
    System.out.println(words[0]+" : "+words[1]+" : "+words[2]);
    MacSrc.set(words[1]);
    wLog.set(MacSrc);
    
    context.write(wLog, one);
  }
  
 }
public static class Sreducer extends Reducer <SMac, IntWritable,NullWritable , Text>
 { 
  private Text MacSrc = new Text();
  private JSONObject obj = new JSONObject();
  private boolean jsons = false ;
  public void setup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("["));  
  }
  public void reduce(SMac key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    MacSrc = key.getMacSrc();
    obj = new JSONObject();
    while(values.iterator().hasNext()){
        System.out.println(values.iterator().next().toString());
        sum++;
        
    }
    try{
        obj.put("MacSrc", key.getMacSrc());
        obj.put("value",sum);
    }catch(JSONException e){
        e.printStackTrace();
    }
    System.out.println(MacSrc);
    if(jsons == false){
        context.write(NullWritable.get(),new Text(obj.toString()));
        jsons = true;
    }else{
        context.write(NullWritable.get(),new Text(","+obj.toString()));
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("]"));  
  }
 }
public static class timemapper extends Mapper <LongWritable, Text, timeMac, IntWritable>
 {
  private timeMac wLog = new timeMac();
  private static final IntWritable one = new IntWritable(1);
  private Text time = new Text();
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split(" ") ;
    
    System.out.println(words[0]+" : "+words[1]+" : "+words[2]);
    time.set(words[0]);
    wLog.set(time);
    
    context.write(wLog, one);
  }
  
 }
public static class timereducer extends Reducer <timeMac, IntWritable,NullWritable , Text>
 { 
  private Text time = new Text();
  private JSONObject obj = new JSONObject();
  private boolean jsons = false ;
  public void setup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("["));  
  }
  public void reduce(timeMac key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    time = key.gettime();
    obj = new JSONObject();
    while(values.iterator().hasNext()){
        System.out.println(values.iterator().next().toString());
        sum++;
        
    }
    try{
        obj.put("time", key.gettime());
        obj.put("value",sum);
    }catch(JSONException e){
        e.printStackTrace();
    }
    System.out.println(time);
    if(jsons == false){
        context.write(NullWritable.get(),new Text(obj.toString()));
        jsons = true;
    }else{
        context.write(NullWritable.get(),new Text(","+obj.toString()));
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("]"));  
  }
 }
public static class Dmapper extends Mapper <LongWritable, Text, DMac, IntWritable>
 {
  private DMac wLog = new DMac();
  private static final IntWritable one = new IntWritable(1);
  private Text MacDst = new Text();
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split(" ") ;
    
    System.out.println(words[0]+" : "+words[1]+" : "+words[2]);
    MacDst.set(words[2]);
    wLog.set(MacDst);
    
    context.write(wLog, one);
  }
  
 }
public static class Dreducer extends Reducer <DMac, IntWritable,NullWritable , Text>
 {
  private Text MacDst = new Text();
  private JSONObject obj = new JSONObject();
  private boolean jsons = false ;
  public void setup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("["));  
  }
  public void reduce(DMac key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    MacDst = key.getMacDst();
    obj = new JSONObject();
    while(values.iterator().hasNext()){
        System.out.println(values.iterator().next().toString());
        sum++;
        
    }
    try{
        obj.put("MacDst", key.getMacDst());
        obj.put("value",sum);
    }catch(JSONException e){
        e.printStackTrace();
    }
    
    System.out.println(MacDst);
    if(jsons == false){
        context.write(NullWritable.get(),new Text(obj.toString()));
        jsons = true;
    }else{
        context.write(NullWritable.get(),new Text(","+obj.toString()));
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("]"));  
  }
 }
public static class Qmapper extends Mapper <LongWritable, Text, QuickDetect, IntWritable>
 {
  private QuickDetect wLog = new QuickDetect();
  private static final IntWritable one = new IntWritable(1);
  private Text time = new Text();
  private Text MacSrc = new Text();
  private Text MacDst = new Text();
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split(" ") ;
    
    System.out.println(words[0]+" : "+words[1]+" : "+words[2]);
    time.set(words[0]);
    MacSrc.set(words[1]);
    MacDst.set(words[2]);
    wLog.set(time,MacSrc,MacDst);
    
    context.write(wLog, one);
  }
  
 }
public static class Qreducer extends Reducer <QuickDetect, IntWritable,NullWritable , Text>
 { 
  private Text time = new Text();
  private Text MacSrc = new Text();
  private Text MacDst = new Text();
  private JSONObject obj = new JSONObject();
  private boolean jsons = false ;
  public void setup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("["));  
  }
  public void reduce(QuickDetect key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    time = key.getTime();
    MacSrc = key.getMacSrc();
    MacDst = key.getMacDst();
    obj = new JSONObject();
    while(values.iterator().hasNext()){
        System.out.println(values.iterator().next().toString());
        sum++;
        
    }
    try{
        obj.put("time", key.getTime());
        obj.put("MacSrc", key.getMacSrc());
        obj.put("MacDst", key.getMacDst());
        obj.put("value",sum);
    }catch(JSONException e){
        e.printStackTrace();
    }
    System.out.println(time+"#"+MacSrc+"->"+MacDst);
    if(jsons == false){
        context.write(NullWritable.get(),new Text(obj.toString()));
        jsons = true;
    }else{
        context.write(NullWritable.get(),new Text(","+obj.toString()));
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("]"));  
  }
 }
public static class Mitmmapper extends Mapper <LongWritable, Text, DetectMitm, IntWritable>
 {
  private DetectMitm wLog = new DetectMitm();
  private static final IntWritable one = new IntWritable(1);
  private Text time = new Text();
    private Text ArpMacSrc = new Text();
    private Text ArpMacDst = new Text();
    private Text ArpIpSrc= new Text();
    private Text ArpIpDst= new Text();
    private Text status= new Text();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split(" ") ;
    
    System.out.println(words[0]+" : "+words[1]+" : "+words[2]);
    time.set(words[0]);
    ArpMacSrc.set(words[4]);
    ArpMacDst.set(words[6]);
    ArpIpSrc.set(words[5]);
    ArpIpDst.set(words[7]);
    status.set(words[9]);
    wLog.set(time,ArpMacSrc,ArpMacDst,ArpIpSrc,ArpIpDst,status);
    
    context.write(wLog, one);
  }
  
 }
public static class Mitmreducer extends Reducer <DetectMitm, IntWritable,NullWritable , Text>
 { 
    private Text time = new Text();
    private Text ArpMacSrc = new Text();
    private Text ArpMacDst = new Text();
    private Text ArpIpSrc= new Text();
    private Text ArpIpDst= new Text();
    private Text status= new Text();
    private JSONObject obj = new JSONObject();
    private boolean jsons = false ;
  public void setup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("["));  
  }
  public void reduce(DetectMitm key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    time = key.getTime();
    ArpMacSrc = key.getArpMacSrc();
    ArpMacDst  = key.getArpMacDst();
    ArpIpSrc = key.getArpIpSrc();
    ArpIpDst = key.getArpIpDst();
    status = key.getstatus();
    obj = new JSONObject();
    while(values.iterator().hasNext()){
        System.out.println(values.iterator().next().toString());
        sum++;
        
    }
    try{
        obj.put("time", key.getTime());
        obj.put("ArpMacSrc", key.getArpMacSrc());
        obj.put("ArpMacDst", key.getArpMacDst());
        obj.put("ArpIpSrc", key.getArpIpSrc());
        obj.put("ArpIpDst", key.getArpIpDst());
        obj.put("status", key.getstatus());
        obj.put("value",sum);
    }catch(JSONException e){
        e.printStackTrace();
    }
    System.out.println(time+"#"+ArpMacSrc+" "+ArpMacDst+" "+ArpIpSrc+" "+ArpIpDst+"X"+status);
    if(jsons == false){
        context.write(NullWritable.get(),new Text(obj.toString()));
        jsons = true;
    }else{
        context.write(NullWritable.get(),new Text(","+obj.toString()));
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("]"));  
  }
 }
public static class ArpScanmapper extends Mapper <LongWritable, Text, DetectArpScan, IntWritable>
 {
  private DetectArpScan wLog = new DetectArpScan();
  private static final IntWritable one = new IntWritable(1);
    private Text time = new Text();
    private Text MacSrc = new Text();
    private Text MacDst = new Text();
    private Text ArpMacSrc = new Text();
    private Text ArpMacDst = new Text();
    private Text ArpIpSrc= new Text();
    private Text ArpIpDst= new Text();
    private Text status= new Text();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split(" ") ;
    
    System.out.println(words[0]+" : "+words[1]+" : "+words[2]);
    time.set(words[0]);
    MacSrc.set(words[1]);
    MacDst.set(words[2]);
    ArpMacSrc.set(words[4]);
    ArpMacDst.set(words[6]);
    ArpIpSrc.set(words[5]);
    ArpIpDst.set(words[7]);
    status.set(words[9]);
    wLog.set(time,MacSrc,MacDst,ArpMacSrc,ArpMacDst,ArpIpSrc,ArpIpDst,status);
    
    context.write(wLog, one);
  }
  
 }
public static class ArpScanreducer extends Reducer <DetectArpScan, IntWritable,NullWritable , Text>
 { 
   private Text time = new Text();
    private Text MacSrc = new Text();
    private Text MacDst = new Text();
    private Text ArpMacSrc = new Text();
    private Text ArpMacDst = new Text();
    private Text ArpIpSrc= new Text();
    private Text ArpIpDst= new Text();
    private Text status= new Text();
    private JSONObject obj = new JSONObject();
    private boolean jsons = false ;
  public void setup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("["));  
  }
  public void reduce(DetectArpScan key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    time = key.getTime();
    MacSrc = key.getMacSrc();
    MacDst = key.getMacDst();
    ArpMacSrc = key.getArpMacSrc();
    ArpMacDst  = key.getArpMacDst();
    ArpIpSrc = key.getArpIpSrc();
    ArpIpDst = key.getArpIpDst();
    status = key.getstatus();
    obj = new JSONObject();
    while(values.iterator().hasNext()){
        System.out.println(values.iterator().next().toString());
        sum++;
    }
    try{
        obj.put("time", key.getTime());
        obj.put("MacSrc", key.getMacSrc());
        obj.put("MacDst", key.getMacDst());
        obj.put("ArpMacSrc", key.getArpMacSrc());
        obj.put("ArpMacDst", key.getArpMacDst());
        obj.put("ArpIpSrc", key.getArpIpSrc());
        obj.put("ArpIpDst", key.getArpIpDst());
        obj.put("status", key.getstatus());
        obj.put("value",sum);
    }catch(JSONException e){
        e.printStackTrace();
    }
    System.out.println(time+"#"+MacSrc+" "+MacDst+" "+ArpMacSrc+" "+ArpMacDst+" "+ArpIpSrc+" "+ArpIpDst+"X"+status);
    if(jsons == false){
        context.write(NullWritable.get(),new Text(obj.toString()));
        jsons = true;
    }else{
        context.write(NullWritable.get(),new Text(","+obj.toString()));
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("]"));  
  }
 }
public static class Srcmapper extends Mapper <LongWritable, Text, DetectSrc, IntWritable>
 {
  private DetectSrc wLog = new DetectSrc();
  private static final IntWritable one = new IntWritable(1);
  private Text time = new Text();
  private Text MacSrc = new Text();
  private Text ArpMacSrc = new Text();
  private Text ArpIpSrc = new Text();
  private Text status = new Text();
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] words = value.toString().split(" ") ;
    
    System.out.println(words[0]+" : "+words[1]+" : "+words[2]);
    time.set(words[0]);
    MacSrc.set(words[1]);
    ArpMacSrc.set(words[4]);
    ArpIpSrc.set(words[5]);
    status.set(words[9]);
    wLog.set(time,MacSrc,ArpMacSrc,ArpIpSrc,status);
    
    context.write(wLog, one);
    System.out.println(key.toString());
  }
  
 }
public static class Srcreducer extends Reducer <DetectSrc, IntWritable,NullWritable , Text>
 {
  private Text time = new Text();
  private Text MacSrc = new Text();
  private Text ArpMacSrc = new Text();
  private Text ArpIpSrc = new Text();
  private Text status = new Text();
  private JSONObject obj = new JSONObject();
  private boolean jsons = false ;
  public void setup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("["));  
  }
  public void reduce(DetectSrc key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    time = key.getTime();
    MacSrc = key.getMacSrc();
    ArpMacSrc = key.getArpMacSrc();
    ArpIpSrc = key.getArpIpSrc();
    status = key.getstatus();
    obj = new JSONObject();
    while(values.iterator().hasNext()){
        System.out.println(values.iterator().next().toString());
        sum++;
        
    }
    try{
        obj.put("time", key.getTime());
        obj.put("MacSrc", key.getMacSrc());
        obj.put("ArpMacSrc", key.getArpMacSrc());
        obj.put("ArpIpSrc", key.getArpIpSrc());
        obj.put("status", key.getstatus());
        obj.put("value",sum);
    }catch(JSONException e){
        e.printStackTrace();
    }
    if(jsons == false){
        context.write(NullWritable.get(),new Text(obj.toString()));
        jsons = true;
    }else{
        context.write(NullWritable.get(),new Text(","+obj.toString()));
    }
  }
  public void cleanup(Context context) throws IOException, InterruptedException{
    context.write(NullWritable.get(),new Text("]"));  
  }
 }
    @Override
    public int run(String[] strings) throws Exception {
      Configuration conf = new Configuration();
      String in = strings[0];
      String out = strings[1];
      FileSystem fs = FileSystem.get(conf);
      if(fs.exists(new Path(out)))
	{
	fs.delete(new Path(out), true);
        fs.delete(new Path(out+"Src"), true);
        fs.delete(new Path(out+"Mitm"), true);
        fs.delete(new Path(out+"ArpScn"), true);
        fs.delete(new Path(out+"s"), true);
        fs.delete(new Path(out+"d"), true);
        fs.delete(new Path(out+"t"), true);
	}
      Job job = new Job();
      Job job2 = new Job();
      Job job3 = new Job();
      Job job4 = new Job();
      Job job5 = new Job();
      Job job6 = new Job();
      Job job7 = new Job();
      job.setJobName("Q");
      job2.setJobName("Src");
      job3.setJobName("Mitm");
      job4.setJobName("ArpScn");
      job5.setJobName("s");
      job6.setJobName("d");
      job7.setJobName("time");
      job.setJarByClass(QuickDetect.class);
      
      job.setMapperClass(Qmapper.class);
      job.setReducerClass(Qreducer.class);
      
      job2.setMapperClass(Srcmapper.class);
      job2.setReducerClass(Srcreducer.class);

      job3.setMapperClass(ArpScanmapper.class);
      job3.setReducerClass(ArpScanreducer.class);
      
      job4.setMapperClass(Mitmmapper.class);
      job4.setReducerClass(Mitmreducer.class);
      
      job5.setMapperClass(Smapper.class);
      job5.setReducerClass(Sreducer.class);
      
      job6.setMapperClass(Dmapper.class);
      job6.setReducerClass(Dreducer.class);
      
      job7.setMapperClass(timemapper.class);
      job7.setReducerClass(timereducer.class);
      //testFinal168.txt
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      
      job2.setOutputKeyClass(NullWritable.class);
      job2.setOutputValueClass(Text.class);  
      
      job3.setOutputKeyClass(NullWritable.class);
      job3.setOutputValueClass(IntWritable.class);
      
      job4.setOutputKeyClass(NullWritable.class);
      job4.setOutputValueClass(Text.class);
      
      job5.setOutputKeyClass(NullWritable.class);
      job5.setOutputValueClass(Text.class);
      
      job6.setOutputKeyClass(NullWritable.class);
      job6.setOutputValueClass(Text.class);
      
      job7.setOutputKeyClass(NullWritable.class);
      job7.setOutputValueClass(Text.class);
      
      job.setMapOutputKeyClass(QuickDetect.class);
      job.setMapOutputValueClass(IntWritable.class);
      //job.setOutputFormatClass(YearMultipleTextOutputFormat.class);
      job2.setMapOutputKeyClass(DetectSrc.class);
      job2.setMapOutputValueClass(IntWritable.class);
      
      job3.setMapOutputKeyClass(DetectArpScan.class);
      job3.setMapOutputValueClass(IntWritable.class);
      
      job4.setMapOutputKeyClass(DetectMitm.class);
      job4.setMapOutputValueClass(IntWritable.class);
      
      job5.setMapOutputKeyClass(SMac.class);
      job5.setMapOutputValueClass(IntWritable.class);
      
      job6.setMapOutputKeyClass(DMac.class);
      job6.setMapOutputValueClass(IntWritable.class);
      
      job7.setMapOutputKeyClass(timeMac.class);
      job7.setMapOutputValueClass(IntWritable.class);
      
      FileInputFormat.addInputPath(job, new Path(in));
      FileOutputFormat.setOutputPath(job, new Path(out));
      if(job.waitForCompletion(true)){
        FileInputFormat.addInputPath(job2, new Path(in));
        FileOutputFormat.setOutputPath(job2, new Path(out+"Src"));
       if(job2.waitForCompletion(true)){
            FileInputFormat.addInputPath(job3, new Path(in));
            FileOutputFormat.setOutputPath(job3, new Path(out+"ArpScn"));      
           if(job3.waitForCompletion(true)){
                FileInputFormat.addInputPath(job4, new Path(in));
                FileOutputFormat.setOutputPath(job4, new Path(out+"Mitm"));
                if(job4.waitForCompletion(true)){
                    FileInputFormat.addInputPath(job5, new Path(in));
                    FileOutputFormat.setOutputPath(job5, new Path(out+"s"));
                    if(job5.waitForCompletion(true)){
                        FileInputFormat.addInputPath(job6, new Path(in));
                        FileOutputFormat.setOutputPath(job6, new Path(out+"d"));
                        if(job6.waitForCompletion(true)){
                            FileInputFormat.addInputPath(job7, new Path(in));
                            FileOutputFormat.setOutputPath(job7, new Path(out+"t"));
                            job7.waitForCompletion(true);
                          }else{
                           return 1;
                         }
                      }else{
                       return 1;
                     }
                  }else{
                   return 1;
                 }
              }else{
               return 1;
             }
          }else{
            return 1;
          }
       }else{
          return 1;
       }
        return 0;
    }
    
}
