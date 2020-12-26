package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.File;

import config.Project;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import map.MapReduce;
import map.Reducer;
import map.Mapper;


public class Job implements JobInterfaceX {

	ArrayList<Worker> workers = new ArrayList<Worker>();
	ArrayList<String> inputFnames = new ArrayList<String>();

	/** Nombre de maps. */
	private int taskMap;
	/** Nombre de reduces. */
	private int taskReduce;
	/** Format d'entree. */
	private Format.Type inputFormat;
	/** Format de sortie. */
	private Format.Type outputFormat;
	/** Comparateur pour trier. */
	private SortComparator sc;
	/** Nom du fichier source. */
	private String inName;
	/** Nom du fichier resultat. */
	private String outName;

	/** Constructeur de Job. */
	public Job() {
		this.taskReduce = 1;
		this.taskMap = workers.size();
		this.inputFormat = Format.Type.KV;
		this.outputFormat = Format.Type.KV;
		this.sc = new SortComparatorImpl(); 
	}

	@Override
	public void setInputFormat(Format.Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setOutputFormat(Format.Type ft) {
		this.outputFormat = ft;		
	}

	@Override
	public void setInputFname(String fname) {
		this.inName = fname;
	}

	@Override
	public void setOutputFname(String fname) {
		this.outName = fname;		
	}

	@Override
	public void setNumberOfReduces(int tasks) {
		this.taskReduce = tasks;		
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.taskMap = tasks;		
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		this.sc = sc;
	}

	@Override
	public Format.Type getInputFormat() {
		return this.inputFormat;
	}

	@Override
	public Format.Type getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
		return this.inName;
	}

	@Override
	public String getOutputFname() {
		return this.outName;
	}

	@Override
	public int getNumberOfReduces() {
		return this.taskReduce;
	}

	@Override
	public int getNumberOfMaps() {
		return this.taskMap;
	}

	@Override
	public SortComparator getSortComparator() {
		return this.sc;
	}

	@Override
	public void startJob (MapReduce mr) {

		String[] name = inName.split("\\.");
		String nom = name[0];
		int repFactor = 1;

		try {
			HdfsClient.HdfsWrite(this.inputFormat, inName, repFactor);
		} catch(Exception e) {
			e.printStackTrace();
		}

		int nombrefragements = workers.size();
		for(int i = 0; i<nombrefragements; i++) {
			inputFnames.add(i, "../Serveur "+i+"/src/" + nom + i + ".txt" );
		}

		setNumberOfReduces(1);
		setNumberOfMaps(nombrefragements);

		//Project.Nbmachines
		for(int i = 0; i<Project.NBHOSTS; i++) {
			try {
				Worker svr = (Worker)Naming.lookup("//"+Project.HOSTS[i]+":"+Project.PORTS[i] + "/worker");
				workers.add(svr);
			} catch (MalformedURLException | RemoteException | NotBoundException e) {
				e.printStackTrace();
			}
		}


		CallBack cb;
		try {
			cb = new CallBackImpl();
			for(int i = 0; i < getNumberOfMaps(); i++) {
				KVFormat writer = new KVFormat("../Serveur"+(i%(Project.NBHOSTS))+"/src/" + "tmp" + i + ".txt");
				KVFormat kvReader = new KVFormat("../Serveur"+(i%(Project.NBHOSTS))+"/src/" + nom + i + ".txt");
				LineFormat lineReader = new LineFormat("../Serveur"+(i%(Project.NBHOSTS))+"/src/" + nom + i + ".txt");

				if (getInputFormat() == Type.KV) {
					workers.get(i).runMap(mr, kvReader, writer, cb);

				} else {
					Worker worker1 = workers.get(i%(Project.NBHOSTS));
					worker1.runMap(mr, lineReader, writer, cb);
					System.out.println("erreur 4");		
				}
			}

			for(int i = 0; i < getNumberOfMaps(); i++) {
				try {				
					cb.decrement();
				} catch (Exception e) {}
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		try {
			HdfsClient.HdfsRead("tmp.txt", "inter-"+inName);
			HdfsClient.HdfsDelete("tmp.txt");
			KVFormat reduceReader = new KVFormat("inter-" + inName);
			KVFormat reduceWriter = new KVFormat(inName+"-res");
			long startTime = System.currentTimeMillis();
			mr.reduce(reduceReader, reduceWriter);
			long endTime = System.currentTimeMillis();
			System.out.println("Temps du Reduce : " + (endTime - startTime));
			File fichier = new File("inter-" + inName); 
			fichier.delete();           
			System.out.println("reduce");     
		} catch (Exception e) {}		
	}

}
