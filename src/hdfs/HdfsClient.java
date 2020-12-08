/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;


public class HdfsClient {

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {
        //recherche des blocs du fichier à supprimer
        
        // mapnode du fichier à supp ( à récupérer du serveur)
        Map<String,String> mapnode_fichier; //à compléter
        // liste des machines où un bloc du fichier existe(values de la mapnode du fichier)
        ArrayList<String> listemachines = new ArrayList<String>(mapnode_fichier.values());
        if (listemachines == null) {
            System.out.println("Fichier inexistant!");
        }
        else{ //on supprime les bouts du fichier dans les différents datanode
          for (String machine : listemachines) {
                String[] x = machine.split("@"); //on récup les coordonnées de la machine
                String adresse = x[0];
                int port = Integer.parseInt(x[1]);
                //HdfsServeur.delete(etc etc)
            }
           // à compléter
        } 
        
    
    
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) { 

     }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
     // récupération du fichier 
     Map<String,String> mapnode_fichier;   
    // vérification si le fichier existe
    if (mapnode_fichier.values().size() ==0) {
        System.out.println("Ce fichier n'existe pas !");
    }
        // récup fichier + appel serveur pour lire
    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],null); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
