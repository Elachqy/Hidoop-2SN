package ordo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.net.InetAddress;

import formats.*;
import hdfs.*;
import map.*;
import application.*;


public class Job extends Thread implements JobInterface {

	// La liste des machines où se trouve un daemon.
	private List<String> listeMachine;

	// La liste des objets RMI deamon, si jamais on souhaite leur faire passer un message.
	private	HashMap<String, String> listeURLWorker;
	private HashMap<String, Worker> listeNode;

	// Informations sur le fichier à traiter 
	private Type InputFormat;
	private String InputFname;

	// Objet Callback
	CallBack cb;

	// Liste des références aux démons du cluster
	private Worker[] listeWorker;

	// Nombre de tâches finies
	int nbTachesFinies;

	// Nombre de workers (daemons)
	int nbWorkers;


	public Job () {
		this.InputFormat = null;
		this.InputFname = null;
		this.listeWorker = HidoopClient.listeWorker;		
		this.nbWorkers = listeWorker.length;
		this.cb = null;
	}


	public void setInputFormat(Type ft) {
		this.InputFormat = ft;
	}


	public void setInputFname(String fname) {
		this.InputFname = fname;
	}


	public int getPortWorkers() {
		return portWorkers;
	}


	public void setPortWorkers(int portWorkers) { // <- On devrait peut être interdire ça... mais c'est bien pratique quand même...
		try {
			listeMachine = new ArrayList<>();
			listeURLWorker = new HashMap<>();
			BufferedReader reader = new BufferedReader(new FileReader("../config/listeMachines.txt"));
			String line;
			while ((line = reader.readLine()) != null)
			{
				listeURLWorker.put(line, ("//" + line + ":"+ portWorkers +"/Worker"));
				listeMachine.add(line);
			}
			reader.close();
		} catch (Exception e) {
			System.err.println("config/listeMachines.txt absent.");
			System.exit(-1);
		}
		this.portWorkers = portWorkers;
	}


	// Récupérer le nombre de fragments du fichier HDFS
	// via le fichier node (objet avec un attribut hashmap)
	private static int recuprerNode(String fname) {		

		int res = 0;

		// Flux d'écriture depuis un fichier (node.txt)
		try {
			// Récupérer l'objet Namenode
			Namenode node = new Namenode();

			// Récupérer le nombre de fragments du fichier
			res = node.getNbFragments(fname);

		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		} 

		return res;
	}


	// Appliquer runMap sur tous les fragments du fichier d'origine
	public void startJob(MapReduce mr) {

		// Chemin d'accès vers les fragments
		String path = "/tmp/data/";

		// Reader ; fragment source (LineFormat dans l'application comptage de mots)
		String fsce;
		Format reader;		
		// Writer ; fragment destination (suffixe "-res") (KVFormat)
		String fdest;
		Format writer;

		String[] nomExt = this.InputFname.split("\\.");

		// Nombre de fragments du fichier
		int nbFragments;
		System.out.println(this.InputFname);
		nbFragments = recupNode(this.InputFname);
		try {
			this.cb = new CallBackImpl(recupNode(this.InputFname));
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}

		// Appliquer le traitement sur tous les fragments :
		// s'il n'y a qu'un fragment :
		try {
			if (nbFragments == 1) {
				// On met le suffixe "_1" pour le premier fragment

				fsce = path + nomExt[0] + "_1" + "." + nomExt[1];
				nomExt = fsce.split("\\.");

				fdest = fsce + "-res";

				reader = new LineFormat(fsce);
				writer = new KVFormat(fdest);

				listeWorker[0].runMap(mr, reader, writer, cb);

				// S'il y a plusieurs fragments :
			} else {
				for (int i = 0 ; i < nbFragments; i++) {
					// Format des noms de fragments : "<nom fichier HDFS>_< n° fragment >"
					fsce = path + nomExt[0] + "_" + i + "." + nomExt[1];

					// Fragment destination : ajouter le suffixe "-res";
					fdest = path + nomExt[0] + "_" + i + "-res" + "." + nomExt[1];

					reader = new LineFormat(fsce);
					writer = new KVFormat(fdest);

					// Compteur : si on atteint la fin de la liste de démons,
					// retourner au début de celle-ci ; ainsi, on parcourt
					// bien les fragments conformément à HDFS

					listeWorker[i%nbWorkers].runMap(mr, reader, writer, cb);
				}	
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
}
