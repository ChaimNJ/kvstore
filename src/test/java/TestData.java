import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestData {

   public static Map<String,Map<String,String>> getMapList(){
       System.out.println("create maplist ...");
       Map<String,Map<String,String>> mapList=new HashMap<>();
       BufferedReader reader = null;
       try {
           reader = new BufferedReader(new FileReader(new File("G:/data/datademo")));
           String line = null;
           while((line=reader.readLine())!=null) {
               Gson gson = new Gson();
               mapList.putAll(gson.fromJson(line, new TypeToken<Map<String,Map<String,String>>>(){}.getType()));
               if(mapList.size()>=10000)
                   break;
           }
       } catch (IOException e) {
           e.printStackTrace();
       }
       System.out.println(mapList.size());
       return mapList;
    }

    public static void write(){
        try {
            File f=new File("G:/data/datademo");
            if(!f.exists()){
                f.createNewFile();
            }
            BufferedWriter w = new BufferedWriter(new FileWriter(f));
            for (int i = 0; i <10000 ; i++) {
                Map<String,String> m=new HashMap<>();
                Map<String,Map<String,String>> map=new HashMap<>();
                m.put(String.valueOf(i),String.valueOf(i));
                m.put(String.valueOf(i+1),String.valueOf(i+1));
                map.put(String.valueOf(i),m);
                w.write(map.toString());
                w.newLine();
            }
            w.flush();
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long t1=new Date().getTime();
        write();
        long t2=new Date().getTime();
        System.out.println("cost:"+(t2-t1));
//            getMapList();
    }
}
