package hdfs;

import java.io.IOException;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import formats.*;


public class HdfsServer {

    public static ArrayList<String> getDataNodeList(Commande cmd, String fileName, Format.Type fmt, int nbFragments){
        ArrayList<String> DataNodeList = new ArrayList<String>();
        try {
            /* Récupération de l'adresse du NameNode */
			InetAddress NameNodeAdress = InetAddress.getByName(NameNode./*NameNodeAdress*/); // à compléter @
			/* Connexion avec le NameNode et récupération des objets de lecture et d'écriture */
			Socket s = new Socket(NameNodeAdress, NameNode./*NameNodePortClient*/); // à compléter
			ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream is = new ObjectInputStream(s.getInputStream());
			/* Envoi de la commande */
			String type;
			if(fmt == Format.Type.LINE) {
				type = "Line";
			} else {
				type = "Kv";
			}
			os.writeObject("CMD_WRITE" + "@" + fileName + "@" + type + "@" + Integer.toString(nbFragments));
			/* Lecture de la liste des DataNodes */
			ArrayList<String> list = (ArrayList<String>) is.readObject();
			DataNodeList = list;
			os.close();
			is.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return DataNodeList;
    }

    /* 
    * Méthode qui contacte le DataNode et lui envoie un fragment à stocker
    * @param numFragment : numéro du fragment
    * @param nodeAdress  : numéro du port du DataNode
    * @param localFSSourceFname : nom de fichier
    * @param chunkFileFmt : le fichier (fragment) à stocker
    * @param fmt :
    * @param bw  : nombre total de bits à écrire
    * @param nbFragments : nombre de fragments
    * @return nombre de bits lus et envoyés au DataNode
    */
    public static long writeChunkInDN(int numFragment, String nodeAdress, int port, String localFSSourceFname, Format chunkFileFmt, Format.Type fmt ,long bw, int nbFragments) {
    	// à complérer
	// ...
    }
	
	
	
	
	
	
}
