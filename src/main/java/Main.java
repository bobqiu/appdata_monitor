import it.sauronsoftware.cron4j.Scheduler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.sql.Time;

/**
 * Created by CLY on 2015/5/27.
 */
public class Main {
    public static Logger logger;

    static {
        PropertyConfigurator.configure(Main.class.getResource("/my_log4j.properties"));
        logger = Logger.getLogger(Main.class);
    }
    public static void main(String[] args){
        Scheduler s = new Scheduler();
        InputStream is = Main.class.getResourceAsStream("/cron4j.setting");
        File temp = null;
        try {
            temp = File.createTempFile("temp",".setting");
            temp.deleteOnExit();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
            String word;
            while((word = br.readLine()) != null){
                bw.write(word);
                bw.newLine();
            }
            br.close();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        s.scheduleFile(temp);
        s.start();
    }
}
