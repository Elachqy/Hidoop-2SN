package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import map.MapReduce;
import ordo.Job;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;

public class TestMapReduce {
	/**
	 * Simple tests de bases. 
	 * Le fichier doit être rendu intégralement
	 * @param args port du worker.
	 */
	public static void main(String[] args) {
		Job job = new Job();
		job.setInputFormat(Format.Type.LINE);
		job.setInputFname("filesample.txt");
		MapReduce map = new MapReduce() {

			@Override
			public void reduce(FormatReader reader, FormatWriter writer) {
				KV kv;
				int i = 0;
				while ((kv = reader.read()) != null) {
					i += Integer.parseInt(kv.v);
				}
				KV resultat = new KV("R", Integer.toString(i));
				writer.write(resultat);
			}

			@Override
			public void map(FormatReader reader, FormatWriter writer) {
				int i = 0;
				KV kv;
				while ((kv = reader.read()) != null) {
					i++;
				}
				KV resultat = new KV("R", Integer.toString(i));
				writer.write(resultat);
			}
		};

		long debut = System.currentTimeMillis();
		job.startJob(map);
		long fin = System.currentTimeMillis();
		System.out.println(fin-debut);
		System.exit(0);
	}
}
