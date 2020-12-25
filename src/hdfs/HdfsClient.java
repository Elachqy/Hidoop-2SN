/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;
import formats.*;


public class HdfsClient {

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String FileName) {
	    
        /* MapNode du fichier à supprimer (à récupérer du serveur) */
        Map<String,String> mapnode_fichier; //à compléter
	    
        // liste des machines où un bloc du fichier existe (values de la mapnode du fichier)
        ArrayList<String> listemachines = new ArrayList<String>(mapnode_fichier.values());
        if (listemachines == null) {
            System.out.println("Fichier inexistant!");
        }
        else{ /* on supprime les bouts du fichier dans les différents datanode */
          for (String machine : listemachines) {
                String[] coordonnees = machine.split("@"); //on récupère les coordonnées de la machine
                String adresse = coordonnees[0];
                int port = Integer.parseInt(coordonnees[1]);
                //HdfsServeur.delete(...)
            }
           // à compléter
        } 
        
    
    
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname) { 
	    Format FileFmt; 	// le format du fichier à envoyer
	    //File fichier = new File(...) récupération du fichier du système local (à compléter)
	    if (fichier.exists()) {
	    	long taille = fichier.length()*8; 	// la taille du fichier en bits
	    
	    	/* Détermination de la taille d'un fragment : */
	    	long tailleFragment;
	    
	    	if (taille <= 1 * (long)(Math.pow(10, 6)) * 8) { /* taille <= 1 Mo */
			tailleFragment = 100 * (long)(Math.pow(10, 3)) * 8;	//la taille d'un fragment est égal à 100 Ko
		    
	    	} else if ( 1 * (long)(Math.pow(10, 6)) * 8 < taille && taille <= 30 * (long)(Math.pow(10, 6)) * 8) { /* 1 Mo <= taille <= 30 Mo */
			tailleFragment = 900 * (long)(Math.pow(10, 3)) * 8;	//la taille d'un fragment est égal à 900 Ko
	    
	    	} else if (30 * (long)(Math.pow(10, 6)) * 8 < taille && taille <= 150 * (long)(Math.pow(10, 6)) * 8){ /* 30 Mo < taille <= 150 Mo */
			tailleFragment = 5 * (long)(Math.pow(10, 6)) * 8;	//la taille d'un fragment est égal à 5 Mo
	    
	    	} else if (150 * (long)(Math.pow(10, 6)) * 8 < taille && taille <= 600 * (long)(Math.pow(10, 6)) * 8){ /* 150 Mo < taille <= 600 Mo */
			tailleFragment = 32 * (long)(Math.pow(10, 6)) * 8;	//la taille d'un fragment est égal à 32 Mo
	    
	    	} else if (600 * (long)(Math.pow(10, 6)) * 8 < taille && taille <= 1500 * (long)(Math.pow(10, 6)) * 8){ /* 600 Mo < taille <= 1.5 Gb */
			tailleFragment = 64 * (long)(Math.pow(10, 6)) * 8;	//la taille d'un fragment est égal à 64 Mo
	    
	    	} else if (1500 * (long)(Math.pow(10, 6)) * 8 < taille && taille <= 6000 * (long)(Math.pow(10, 6)) * 8){ /* 1.5 Gb  < taille <= 6 Gb */
			tailleFragment = 100 * (long)(Math.pow(10, 6)) * 8;	//la taille d'un fragment est égal à 100 Mo
	    
	    	} else {
			tailleFragment = 130 * (long)(Math.pow(10, 6)) * 8;	//sinon la taille d'un fragment est égal à 130 Mo
	    	}	
	    
	    	/* Calcul du nombre de fragments : */
	    	int nbFragments = 0;
	    	if (fmt == Format.Type.LINE) {
			    FileFmt = new LineFormat(...); //à compléter (Projet.fichierSource?)
	    	} else {
			    FileFmt = new KVFormat(...); //à compléter (Projet.fichierSource?)
	    	}
	    	FileFmt.open(Format.OpenMode.R);
	    	KV FileKV = FileFmt.read();
	    	long compteur;
	    	boolean continuer = true;
	    	while(continuer) {
			    if (FileKV == null) {
				    continuer = false;
		    	} else {
				    while(compteur<tailleFragment) {
					    FileKV = file.read();
				    	if (FileKV == null) {
					        continuer = false;
					    	break;
				    	} else if (fmt == Format.Type.LINE) {
					    	compteur += FileKV.v.length()*8;
				    	} else {
					    	compteur += (FileKV.k + KV.SEPARATOR + FileKV.v).length()*8;
				    	}
			     	}   
			     	nbFragments++
			     	compteur = 0;	// on rinitialise à 0 après chaque fragment compté
		    	}
	     	}
	     	FileFmt.close();
	    	/* Récupération de la liste des DataNode où on va envoyer les fragments sous la forme "adress@port" */
		// à compléter
		/* Lancer un thread pour écrire les fragments dans les DataNodes */
		// à compléter
		// ...
		    
	   } else {
		    fichier.mkdir();
		    System.out.println("Fichier inexistant!");
	   }
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
