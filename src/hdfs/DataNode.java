package hdfs;


import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class DataNode {
    public int port;
    public Map<String, Integer> listeBlocs;

    public DataNode(int port) {
        this.port = port;
        this.listeBlocs = new HashMap<String, Integer>();
    }

    public static void main(String[] args) {
        // on commence par établir la connexion entre DataNode et NameNode
        // récupération de l'adresse de la machine
        try {
        InetAddress adressemachine = InetAddress.getLocalHost(); // Inet4 vu qu'on utilise Ipv4
        // récupération de l'adresse du namenode
        InetAddress adresseNM = InetAddress.getByName(NameNode.NameNodeAdresse);
        // on établit la connexion
        String serveur; // path du serveur à insérer !!!!!!
		Socket s1 = new Socket(serveur , NameNode.portNNDN);
        }    


    }









}