package config;

import java.util.List; 
import java.util.ArrayList; 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class Project {

	public final static String PATH = "";
	public static String[] HOSTS ; 
	public  static Integer NBHOSTS ;
	public static int Nbmachines ;
    	public final static int[] PORTS = {2020,2021};
	public static List<String> list = new ArrayList<String>();
    //public static HashMap<String, Integer> list = new HashMap<>();

	public static void main(String args[]) {

		try {
			File myfile = new File(args[0]);
			BufferedReader read = new BufferedReader(new FileReader(myfile));
			String num = read.readLine();
			int numport = Integer.parseInt(num); 
			System.out.println("port initiale : "+ numport );
			String ligne;

			while ((ligne = read.readLine() )!= null) {
				System.out.println("serveur : "+ ligne );
				list.add(ligne);                   
			}

			HOSTS = new String[list.size()]; 
			Nbmachines = list.size();
			NBHOSTS = (Integer) Nbmachines;
			HOSTS = list.toArray(HOSTS);
			System.out.println("Nb machines : "+ Nbmachines );
			System.out.println( HOSTS[0] );

		} catch(Exception e) {
			e.printStackTrace();
		}
	} 
}
