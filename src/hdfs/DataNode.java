package hdfs;

import java.util.HashMap;
import java.util.Map;

public class DataNode {
    public int port;
    public Map<String,Integer> listeBlocs; 

    public DataNode(int port) {
        this.port = port;
        this.listeBlocs = new HashMap<String,Integer>(); 
    }










}