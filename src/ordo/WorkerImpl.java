package ordo;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import formats.Format;
import formats.Format.OpenMode;
import map.Mapper;
import map.Reducer;


public class WorkerImpl extends UnicastRemoteObject implements Worker {

	public WorkerImpl() throws RemoteException{
		super();
	}


	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

		System.out.println("Worker : Commencer une tache");
		System.out.println("Worker : Ouvrir le lecteur");
		reader.open(OpenMode.R);
		System.out.println("Worker : Ouvrir le rédacteur");
		writer.open(OpenMode.W);
		System.out.println("Lancement du Mapper ... ");
		/* Execution du map */
		m.map(reader, writer);
		System.out.println("Execution de map locale terminee avec succes ....");
		/* Fermeture du writer */
		writer.close();
		/* Fermeture du reader */
		reader.close();
		try {
			/* Envoi du CallBack */
			cb.increment();
			System.out.println("CallBack envoyé avec succes");
		} catch (Exception e) {}
		System.out.println("Worker : Fin de la tache");
	}


	@Override
	public void runReduce(Reducer r, Format reader, Format writer) throws RemoteException {

		System.out.println("Worker : Commencer une tache");
		System.out.println("Worker : Ouvrir le lecteur");
		reader.open(OpenMode.R);
		System.out.println("Worker : Ouvrir le rédacteur");
		writer.open(OpenMode.W);
		System.out.println("Lancement du Reducer ... ");
		/* Execution du reduce */
		r.reduce(reader, writer); 
		/* Fermeture du reader */
		reader.close();
		/* Fermeture du writer */
		writer.close();
}



public static void main(String[] args) {
	try {
		int port = Integer.parseInt(args[0]);
		LocateRegistry.createRegistry(port);
		Worker serveur = new WorkerImpl();
		String URL = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/worker";
		Naming.rebind(URL, serveur);
	} catch (RemoteException e) {
		e.getCause();
		e.printStackTrace();
	} catch (MalformedURLException e) {
		e.getCause();
		e.printStackTrace();
	} catch (UnknownHostException e) {
		e.getCause();
		e.printStackTrace();
	}
}

}
