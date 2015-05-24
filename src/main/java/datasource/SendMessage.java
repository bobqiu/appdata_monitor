package datasource;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by CLY on 2015/5/24.
 */
public class SendMessage {
    public static Logger logger;
    static{
        PropertyConfigurator.configure(SendMessage.class.getResource("/my_log4j.properties"));
        logger = Logger.getLogger(SendMessage.class);
    }
    public static final String FILE_PATH = "/var/log/appdata_monitor/data";
    public static final long DAY = 1000 * 60 * 60 * 24;
    public static String package_name;
    public static String file_name;

    public static void main(String [] args) throws ParseException {
        // 1. 从配置文件中读出这次将要读取的数据文件
        File f = new File(FILE_PATH);
        String str = null;
        if (f.exists()){
            String [] arr = readFileName().split(" ");
            package_name = arr[0];
            file_name = arr[1];
        }else{
            package_name = "20130313";
            file_name = "0";
            str = package_name + " " + file_name;
            wtiteFileName(str);
        }
        System.out.println(String.format("[开始]读取%s日%s时的数据，开始发送", package_name, file_name));
        sendMessage();
        System.out.println(String.format("[结束]读取%s日%s时的数据，发送完成", package_name, file_name));
        wtiteFileName(createNewFilename());
    }
    public static String readFileName(){
        File file = new File(FILE_PATH);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String str = reader.readLine();
            reader.close();
            if (str != null){
                return str;
            }
        } catch (IOException e) {
            System.out.println("读取数据文件名称异常");
        }
        return null;
    }
    public static void wtiteFileName(String str){
        FileWriter writer = null;
        try {
            writer = new FileWriter(FILE_PATH);
            writer.write(str);
            writer.close();
        } catch (IOException e) {
            System.out.println("写入数据文件名称异常");
        }
    }
    public static void sendMessage(){
        String path = "/data/" + package_name + "/" + file_name + ".txt";
        BufferedReader br = new BufferedReader(new InputStreamReader(SendMessage.class.getResourceAsStream(path)));
        String words;
        try {
            while((words=br.readLine())!=null){
               logger.info(words);
            }
        } catch (IOException e) {
            System.out.println("向指定端口发送数据异常");
        }

    }
    public static String createNewFilename() throws ParseException {
        String str = null;
        if (file_name.equals("23")){
            Date date = new Date(StringToDate(package_name).getTime() + DAY );
            package_name = DateToString(date);
            file_name = "0";
            str = package_name + " " + file_name;
        }else{
            int fn = Integer.parseInt(file_name) + 1;
            file_name = String.valueOf(fn);
            str = package_name + " " + file_name;
        }
        return str;
    }
    public static Date StringToDate(String str) throws ParseException {
        // String -> Date
        Date date=new Date();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        date=sdf.parse(str);
        return date;
    }
    public static String DateToString(Date date){
        // Date -> String
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String str = sdf.format(date);
        return str;
    }
}
