package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

	private Semaphore call;

	public CallBackImpl() throws RemoteException {
		this.call = new Semaphore(0);
	}

	@Override
	public void increment() throws RemoteException{
		this.call.release();
	}

	@Override
	public void decrement() throws RemoteException, InterruptedException {
		try {
			this.call.acquire();
		} catch (InterruptedException erreur) {
			erreur.printStackTrace();
		}
	}
}

