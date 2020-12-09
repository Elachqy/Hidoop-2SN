package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface NameNodeInterface extends Remote {
    public int portClient = 1000; // port pour communiquer avec le client
    public int portServeur = 2000; // port pour communiquer avec le serveur
    
    public String NameNodeAdresse =""; // adresse du namenode à remplir 

    public Map<String,INode> listeIN = null; // liste des INodes
    public Map<Integer,String> listeDN = null; //liste des DataNodes sous la forme (n*bloc,adresse datanode) 
	public Map<Integer, String> getListeDN() throws RemoteException;
    public Map<String, INode> getListeIN() throws RemoteException;
    public void addDN(int numBloc, String adress) throws RemoteException;



}