package cn.xuan.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class HDFSUtil {

    private Configuration conf = null;

    private FileSystem fs = null;

    public HDFSUtil(String hdfsurl) throws IOException{
        Configuration conf=new Configuration();
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
        fs = FileSystem.get(URI.create(hdfsurl), conf);
    }

    public FSDataOutputStream getHDFSfFileOutStream(String path) throws  IOException{
        FSDataOutputStream fsdos = null;
        Path p = new Path(path);
        if(fs.exists(p)) {
            fsdos = fs.append(p);
        }else{
            fsdos=fs.create(p);
        }
        return fsdos;
    }
    public void closeOutStream(FSDataOutputStream fsdos) throws IOException {
        fsdos.close();
    }

    public Map<String,Long> write_to_hdfs(String data, FSDataOutputStream fsdos) throws  IOException{
        long start_pos=fsdos.getPos();
        fsdos.write(data.getBytes());
        long end_pos=fsdos.getPos();
        fsdos.flush();
        Map<String,Long> map=new HashMap<>();
        map.put("start_pos",start_pos);
        map.put("length",end_pos-start_pos);
        return map;
    }
    public String read_from_hdfs(String path, long start_pos, long length) throws  IOException{
        Path p = new Path(path);
        FSDataInputStream fsdis = fs.open(p);
        byte[] buffer = new byte[new Long(length).intValue()];
        fsdis.seek(start_pos);
        int readLength = fsdis.read(buffer);
        fsdis.close();
        if(readLength == 0){
            return null;
        }else{
            return  new String(buffer);
        }
    }
    public void write_index_to_hdfs(Map<String,String> tmpIndex,FSDataOutputStream fsdos) throws IOException{
        OutputStreamWriter out=new OutputStreamWriter(fsdos);
        BufferedWriter bufferedWriter = new BufferedWriter(out);
        for(Map.Entry<String,String> entry :tmpIndex.entrySet()){
            bufferedWriter.write(entry.getKey()+";"+entry.getValue());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        out.close();
    }

    public Map<String,String> read_index_from_hdfs(String path) throws  IOException {
        Map<String,String> index= new HashMap<>();
        Path p = new Path(path);
        if (fs.exists(p)) {
            FSDataInputStream fsdis = fs.open(p);
            InputStreamReader inputStreamReader = new InputStreamReader(fsdis);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = null;
            String[] linesplit = null;
            while ((line = bufferedReader.readLine()) != null) {
               linesplit=line.split(";");
                if (linesplit.length == 2) {
                    index.put(linesplit[0], linesplit[1]);
                }
            }
            bufferedReader.close();
            inputStreamReader.close();
            fsdis.close();
        }
        return index;
    }

//    public static void main(String[] args) {
//        String url="hdfs://192.168.1.104:9000";
//        try {
//            HDFSUtil hdfs=new HDFSUtil(url);
////            hdfs.getHDFSfFileStream("/store1");
////            System.out.println(hdfs.write_to_hdfs("123456789"));
//            Map<String,String> re=hdfs.read_index_from_hdfs("/index1");
//            System.out.println(re.get("679913"));
//            System.out.println(re.size());
//            System.out.println(hdfs.read_from_hdfs("/store1",11368999,39));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}
