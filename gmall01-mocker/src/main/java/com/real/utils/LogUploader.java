package com.real.utils;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/***
 * 通过http方法发送到采集系统的web端口
 */
public class LogUploader {

    public static void sendLogStream(String log) {
        try {
//            URL url = new URL("http://192.168.159.102:8081/log");

            URL url = new URL("http://192.168.159.102/log");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("POST");

            //时间头为server进行时钟矫正
            conn.setRequestProperty("clientTime", String.valueOf(System.currentTimeMillis()));

            //允许上传数据
            conn.setDoOutput(true);

            //设置请求的头信息,设置内容类型为JSON
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("Upload: " + log);

            //输出流
            OutputStream out = conn.getOutputStream();
            /*关键一行，定义了RequestParam的具体名字进行对应*/
            out.write(("log=" + log).getBytes());
            out.flush();
            out.close();

            int code = conn.getResponseCode();
            System.out.println(code);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
