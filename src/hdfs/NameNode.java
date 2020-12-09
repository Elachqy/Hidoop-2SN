package hdfs;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

public class NameNode extends UnicastRemoteObject implements NameNodeInterface {


	protected NameNode() throws RemoteException {
		NameNode.listeIN = new HashMap<String, INode>();
		NameNode.listeDN = new HashMap<Integer,String>();
		
	}

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Map<Integer, String> getListeDN() throws RemoteException {
		
		return listeDN;
	}

	@Override
	public Map<String, INode> getListeIN() throws RemoteException {
		
		return listeIN;
	}

	@Override
	public void addDN(int numBloc, String adress) throws RemoteException {
		listeDN.put(numBloc, adress);
	}
	//public static void main(String[] args) 

	
} 