package ordo;

import java.io.*;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.Scanner;
import java.io.IOException;

import config.Project;
import application.MyMapReduce;
import map.MapReduce;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.HdfsClient;


public class HidoopClient {

	// Liste des références aux workers du cluster dans le registre.
	public static Worker listeWorker[];

	// Récupérer les emplacements indiqués dans le fichier de configuration
	private static String[] recupererURL(int nbMachines) {

		String path = "src/config/config_hidoop.cfg";
		File fichier = new File(path);
		int cpt = 0;

		String[] ports = new String[nbMachines];
		String[] noms = new String[nbMachines];
		String[] urls = new String[nbMachines];

		BufferedReader buffer;
		try {
			buffer = new BufferedReader(new FileReader(fichier));
			String str; 
			while ((str = buffer.readLine()) != null) {
				// Si la ligne n'est pas un commentaire
				if (!str.startsWith("#")) {
					// Noms des machines
					if (cpt == 0) {
						noms = str.split(",");
					}
					// Ports RMI
					if (cpt == 2) {
						ports = str.split(",");
					}
					cpt++;					  
				}
			}
			buffer.close();

			// Si le fichier de configuration est correct
			if (noms.length != 0 && ports.length == noms.length) {
				for (int i=0 ; i < nbMachines ; i++) {
					urls[i] = "//" + noms[i] + ":" + ports[i] + "/worker";
				}
			} else {
				System.out.println("Utilisation du fichier de configuration :"
						+ "L1 : machine1,machine2"
						+ "L2 : port_hdfs1,port_hdfs2"
						+ "L3 : ports registres RMI"
						+ "L4 : taille");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return urls;
	}


	public static void main (String args[]) throws RemoteException {

		// Formats de fichiers utilisables
		String[] formats = {"line","kv"};

		// Fichiers source 
		Format reader;
		// Fichiers destination
		Format writer;
		// Nombre de machines à utiliser
		int nbCluster;
		// Informations de format de fichier
		Type ft;
		// Liste des url correspondant aux démons du cluster
		String urlWorker[];

		try {
			// Vérifier le bon usage du client
			if (args.length < 3) {
				System.out.println("Utilisation : java HidoopClient fichier format nbmachines");
				System.exit(1);
			} else {
				if (!Arrays.asList(formats).contains(args[1])) {
					System.out.println("Utilisation : java HidoopClient fichier format nbmachines");
					System.exit(1);
				}
			}

			// Nom du fichier sur lequel appliquer le traitement
			String hdfsFname = args[0];

			// Fichier HDFS destination  : ajout du suffixe "-res"
			// Nom du fichier traité avant application du reduce
			String[] nomExt = hdfsFname.split("\\.");
			String localFSDestFname = "data/" + nomExt[0] + "-res" + "." + nomExt[1];

			// Fichier résultat du reduce : ajout du suffixe "-red"
			// Nom du fichier traité après application du reduce
			String reduceDestFname = "data/" + nomExt[0] + "-red" + "." + nomExt[1];

			// Récupérer le format de fichier indiqué en argument
			if (args[1].equals("line")) {
				ft = Format.Type.LINE;
			} else {
				ft = Format.Type.KV;
			}

			// Récupérer le nombre de machines
			nbCluster = Integer.parseInt(args[2]);
			// Récupérer les URLs depuis le fichier de configuration 
			urlWorker = recupererURL(nbCluster);
			// Récupérer les références des objets Worker distants
			// à l'aide des url (déjà connues)
			listeWorker = new Worker[nbCluster];
			for (int i = 0 ; i < nbCluster ; i++) {
				listeWorker[i]=(Worker) Naming.lookup(urlWorker[i]);
			}

			// Création et définition des attributs de l'objet Job
			// On donne la liste des références aux Workers à l'objet Job
			Job job = new Job();

			// Indiquer à job le nom et format du fichier à traiter
			job.setInputFname(hdfsFname);
			job.setInputFormat(ft);

			// Création de l'objet MapReduce
			MyMapReduce mr = new MyMapReduce();

			// Lancement des tâches
			System.out.println("Lancement du Job");
			job.startJob(mr);
			System.out.println("Fin du lancement du Job");

			// Récupérer le fichier traité via HDFS
			HdfsClient.HdfsRead(hdfsFname, localFSDestFname);
			System.out.println("Lecture terminée");

			// Reader : fichier local après traitement sur les machines du cluster
			// Writer : fichier final généré après application du reduce
			reader = new KVFormat(localFSDestFname);
			writer = new KVFormat(reduceDestFname);
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);

			// Appliquer reduce sur le résultat
			// Traitement du fichier résultant des traitements des fragments.
			// Reader : format kv  
			// Writer : format kv
			System.out.println("Début du reduce");
			long startT = System.currentTimeMillis();
			mr.reduce(reader, writer);
			long endT = System.currentTimeMillis();
			System.out.println("Temps du Reduce : " + (endT - startT));
			System.out.println("Fin du reduce");

			reader.close();
			writer.close();

			System.exit(0);

		} catch (RemoteException e) {
			e.printStackTrace();

		} catch (NotBoundException e) {
			e.printStackTrace();

		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
}
