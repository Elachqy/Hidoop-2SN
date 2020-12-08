package hdfs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class INode implements Serializable {
    private static final long serialVersionUID = 1L;
    private String filename;
    private Map<String,String> mapnode;

    public INode(String filename, Map<String,String> mapnode) {
        filename = this.filename;
        mapnode = this.mapnode;

    }
    public INode() {
        this.mapnode = new HashMap<String,String>;
    }
    public String getfilename() {
        return filename;
    }
    public Map<String,String> getmapnode() {
        return mapnode;
    }
    public void setfilename(String newname) {
            this.filename = newname;
    }
    public void setnamenode(Map<String,String> newmapnode) {
        // ne sert à rien  priori 
    }
    
}