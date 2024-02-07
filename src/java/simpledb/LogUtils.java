package simpledb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * @author: yolopluto
 * @Date: created in 2024/2/6 20:40
 * @description: 日志工具
 * @Modified By:
 */
public class LogUtils {
    public final static String INFO = "logInfo.txt";
    public final static String TEST = "logTest.txt";
    public final static String ERROR = "logError.txt";

    public static void print(Object ...objs){
        StringBuilder stringBuilder = new StringBuilder();
        boolean flag = true;
        for (Object obj : objs) {
            if(flag){
                stringBuilder.append((obj)).append(":");
                flag = false;
            }else {
                stringBuilder.append(obj).append("; ");
                flag = true;
            }

        }
        System.out.println(stringBuilder);
    }

    // 追加写入日志
    public static void writeLog(String fileName ,String s){
        File f = new File(fileName);
        if (!f.exists()) {
            try {
                f.createNewFile();// 不存在则创建
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        BufferedWriter output = null;//true,则追加写入text文本
        try {
            output = new BufferedWriter(new FileWriter(f,true));
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            String cur = df.format(System.currentTimeMillis());

            output.write("「"+cur+"」"+s);
            output.write("\r\n");//换行
            output.flush();
            output.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
