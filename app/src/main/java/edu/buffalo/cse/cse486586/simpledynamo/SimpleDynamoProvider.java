package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	public String mySuccesorID = new String();
	public String myPredecessorID = new String();
	public String myHashID = new String();
	public String myPortID = new String();
	public HashMap<String,String> hashPortMap = new HashMap<String, String>();
	public ArrayList<String> hashValues = new ArrayList<String>();

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {

		String filename = selection;
		File filesDir = getContext().getFilesDir();

		String[] myfiles = filesDir.list();
		if(filename.equals("@")){
			if(myfiles != null){
				for(String file : myfiles) {
					File fileToDelete = new File(filesDir,file);
					fileToDelete.delete();
				}
			}
		}
		else{
			// open connection to the avds storing that key
			String coord = getCoordinator(filename);
			String[] coordSplitted = coord.split(",");
			for(String tempCoord : coordSplitted){
				String msgToSend = "3D" + "," + filename;
				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(tempCoord) * 2)), 1000);
					DataOutputStream msgout = new DataOutputStream(socket.getOutputStream());
					DataInputStream msgin = new DataInputStream(socket.getInputStream());

					msgout.writeUTF(msgToSend);
					String msgRead = msgin.readUTF();
					while (!(msgRead.equals("msgrcvd"))) {

					}
					msgin.close();
					msgout.close();
					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "delete operation UnknownHostException");
				} catch (SocketTimeoutException ste) {
					Log.e(TAG, "delete operation SocketTimeoutException");
				} catch (IOException e) {
					Log.e(TAG, "delete operation IOException");
				}
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values)  {

			String filename = (String) values.get("key");
			String value = (String) values.get("value");

			//Find coordinator and replica nodes for the key
			String nodeInfo = getCoordinator(filename);
			String[] nodeInfoSplitted = nodeInfo.split(",");

			//SEND key value to the required nodes
			String msgToSend = "0KV" + "," + filename + "," + value;
			for (String nodeToSend : nodeInfoSplitted) {
				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(nodeToSend) * 2)), 1000);
					DataOutputStream msgout = new DataOutputStream(socket.getOutputStream());
					DataInputStream msgin = new DataInputStream(socket.getInputStream());

					msgout.writeUTF(msgToSend);
					String msgRead = msgin.readUTF();
					msgin.close();
					msgout.close();
					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "insert operation UnknownHostException");
				} catch (SocketTimeoutException ste) {
					Log.e(TAG, "insert operation SocketTimeoutException");
				} catch (IOException e) {
					Log.e(TAG, "insert operation IOException");
				}
			}
			return null;

	}

	@Override
	public boolean onCreate() {
			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			final String myPort = String.valueOf(Integer.parseInt(portStr));
			try {
				myPortID = myPort;
				myHashID = genHash(myPort);

				String port4 = genHash("5554");
				String port6 = genHash("5556");
				String port8 = genHash("5558");
				String port0 = genHash("5560");
				String port2 = genHash("5562");

				hashPortMap.put(port4, "5554");
				hashPortMap.put(port6, "5556");
				hashPortMap.put(port8, "5558");
				hashPortMap.put(port0, "5560");
				hashPortMap.put(port2, "5562");

				hashValues.add(port0);
				hashValues.add(port2);
				hashValues.add(port4);
				hashValues.add(port6);
				hashValues.add(port8);
				Collections.sort(hashValues);
			} catch (Exception e) {
				Log.e(TAG, "NoSuchAlgorithm: genHash");
			}

			int myIndex = hashValues.indexOf(myHashID);
			if (myIndex == 0) {
				mySuccesorID = (String) hashPortMap.get((String) hashValues.get(myIndex + 1));
				myPredecessorID = (String) hashPortMap.get((String) hashValues.get(hashValues.size() - 1));
			} else if (myIndex == (hashValues.size() - 1)) {
				mySuccesorID = (String) hashPortMap.get((String) hashValues.get(0));
				myPredecessorID = (String) hashPortMap.get((String) hashValues.get(hashValues.size() - 2));
			} else {
				mySuccesorID = (String) hashPortMap.get((String) hashValues.get(myIndex + 1));
				myPredecessorID = (String) hashPortMap.get((String) hashValues.get(myIndex - 1));
			}


			try {
				new onCreateKeys().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
					ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
					new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			} catch (IOException e) {
				Log.e(TAG, "Can't create a ServerSocket");
				return false;
			}

			return true;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

			Context context = getContext();
			String filename = selection;
			String[] columnames = {"key", "value"};
			MatrixCursor matrixCursor = new MatrixCursor(columnames);

			if (filename.equals("*")) {
				Integer portToSend = 5554;
				String allKeys = new String();
				String allValues = new String();
				for (int i = 0; i < 5; i++) {
					String msgToSend = "1Q@" + "," + filename;
					try {
						Socket socket = new Socket();
						socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), ((portToSend) * 2)), 1000);
						DataOutputStream msgout = new DataOutputStream(socket.getOutputStream());
						DataInputStream msgin = new DataInputStream(socket.getInputStream());
						msgout.writeUTF(msgToSend);
						String msgRead = msgin.readUTF();
						String[] msgReadSplitted = msgRead.split(",");
						while (!msgReadSplitted[0].equals("msgrcvd")) {

						}
						allKeys = msgReadSplitted[1];
						allValues = msgReadSplitted[2];
						msgin.close();
						msgout.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "query operation UnknownHostException");
					} catch (SocketTimeoutException ste) {
						Log.e(TAG, "query operation SocketTimeoutException");
					} catch (IOException e) {
						Log.e(TAG, "query operation IOException");
					}

					if (!(allKeys.isEmpty())) {
						String[] keyStrSplitted = allKeys.split(":");
						String[] valStrSplitted = allValues.split(":");
						if (keyStrSplitted.length == valStrSplitted.length) {
							for (int j = 0; j < keyStrSplitted.length; j++) {
								Object[] columnvalues = new Object[2];
								columnvalues[0] = keyStrSplitted[j];
								columnvalues[1] = valStrSplitted[j];
								matrixCursor.addRow(columnvalues);
							}
						} else {
							Log.e(TAG, "length of key and value string does not match");
						}
					}

					portToSend = portToSend + 2;

				}

			} else if (filename.equals("@")) {
				String allKeys = new String();
				String allValues = new String();
				String msgToSend = "1Q@" + "," + filename;
				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(myPortID) * 2)), 1000);
					DataOutputStream msgout = new DataOutputStream(socket.getOutputStream());
					DataInputStream msgin = new DataInputStream(socket.getInputStream());
					msgout.writeUTF(msgToSend);
					String msgRead = msgin.readUTF();
					String[] msgReadSplitted = msgRead.split(",");
					while (!msgReadSplitted[0].equals("msgrcvd")) {

					}
					allKeys = msgReadSplitted[1];
					allValues = msgReadSplitted[2];
					msgin.close();
					msgout.close();
					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "query operation UnknownHostException");
				} catch (SocketTimeoutException ste) {
					Log.e(TAG, "query operation SocketTimeoutException");
				} catch (IOException e) {
					Log.e(TAG, "query operation IOException");
				}

				if (!(allKeys.isEmpty())) {
					String[] keyStrSplitted = allKeys.split(":");
					String[] valStrSplitted = allValues.split(":");
					if (keyStrSplitted.length == valStrSplitted.length) {
						for (int i = 0; i < keyStrSplitted.length; i++) {
							Object[] columnvalues = new Object[2];
							columnvalues[0] = keyStrSplitted[i];
							columnvalues[1] = valStrSplitted[i];
							matrixCursor.addRow(columnvalues);
						}
					} else {
						Log.e(TAG, "length of key and value string does not match");
					}
				}

			} else {
				//find the coordinator and replica for the key
				String nodeInfo = getCoordinator(filename);
				String[] nodeInfoSplitted = nodeInfo.split(",");
				String msgToSend = "1Q" + "," + filename;
				Long maxVersion = 0L;
				String value = new String();
				for (String nodeToSend : nodeInfoSplitted) {
					try {
						Socket socket = new Socket();
						socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(nodeToSend) * 2)), 1000);
						DataOutputStream msgout = new DataOutputStream(socket.getOutputStream());
						DataInputStream msgin = new DataInputStream(socket.getInputStream());
						msgout.writeUTF(msgToSend);
						String msgRead = msgin.readUTF();
						String[] msgReadSplitted = msgRead.split(",");
						while (!msgReadSplitted[0].equals("msgrcvd")) {

						}

						if (!(msgReadSplitted[1].equals("empty"))) {
							String[] valueReadSplitted = msgReadSplitted[1].split("\\.");
							Long version = Long.parseLong(valueReadSplitted[1]);
							if (version >= maxVersion) {
								maxVersion = version;
								value = valueReadSplitted[0];
							}
						}
						msgin.close();
						msgout.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "query operation UnknownHostException");
					} catch (SocketTimeoutException ste) {
						Log.e(TAG, "query operation SocketTimeoutException");
					} catch (IOException e) {
						Log.e(TAG, "query operation IOException");
					}

				}
				Object[] columnvalues = new Object[2];
				columnvalues[0] = filename;
				columnvalues[1] = value;
				matrixCursor.addRow(columnvalues);

			}

			return matrixCursor;

	}

	public String getCoordinator(String keySent){
		String key = keySent;
		String keyHashValue = new String();
		String coordinator = new String();
		String coordinatorHash = new String();
		String replica1 = new String();
		String replica2 = new String();

		try{
			keyHashValue = genHash(key);
		}
		catch (Exception e){
			Log.e(TAG,"keyHashValue generation problem");
		}

		for(int i=0; i < hashValues.size(); i++){
			if(i==0){
				int tempComparedKM = keyHashValue.compareTo((String) hashValues.get(i));
				int tempComparedKP = keyHashValue.compareTo((String) hashValues.get(hashValues.size()-1));

				if((tempComparedKP > 0)||(tempComparedKM <= 0)){
					coordinator = (String) hashPortMap.get( (String) hashValues.get(i));
					coordinatorHash = ( (String) hashValues.get(i));
					break;
				}
			}

			else{
				int tempComparedKM = keyHashValue.compareTo((String) hashValues.get(i));
				int tempComparedKP = keyHashValue.compareTo((String) hashValues.get(i-1));

				if((tempComparedKP > 0)&&(tempComparedKM <= 0)){
					coordinator = (String) hashPortMap.get(( (String) hashValues.get(i)));
					coordinatorHash = ( (String) hashValues.get(i));
					break;
				}
			}
		}

		int index = hashValues.indexOf(coordinatorHash);
		if(index==hashValues.size()-1){
			replica1 = hashPortMap.get((String) hashValues.get(0));
			replica2 = hashPortMap.get((String) hashValues.get(1));
		}
		else if(index==hashValues.size()-2){
			replica1 = hashPortMap.get((String) hashValues.get(hashValues.size()-1));
			replica2 = hashPortMap.get((String) hashValues.get(0));
		}
		else{
			replica1 = hashPortMap.get((String) hashValues.get(index +1));
			replica2 = hashPortMap.get((String) hashValues.get(index +2));
		}
		String strToReturn = coordinator + "," + replica1 + "," + replica2;
		return strToReturn;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private class onCreateKeys extends AsyncTask<Void,Void,Void> {
		@Override
		protected Void doInBackground(Void... para) {
			File filesDir = getContext().getFilesDir();
			String msgToSend = "2FRQ" + "," + "@";
			StringBuilder keystr = new StringBuilder();
			StringBuilder valstr = new StringBuilder();

			Integer portToSend = 5554;
			for (int i = 0; i < 5; i++) {

				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), ((portToSend) * 2)), 1000);
					DataOutputStream msgout = new DataOutputStream(socket.getOutputStream());
					DataInputStream msgin = new DataInputStream(socket.getInputStream());
					msgout.writeUTF(msgToSend);
					String msgRead = msgin.readUTF();
					String[] msgReadSplitted = msgRead.split(",");
					while (!msgReadSplitted[0].equals("msgrcvd")) {

					}
					int emptyCheck = msgReadSplitted[1].compareTo("empty");
					if(emptyCheck!=0){
						keystr.append(msgReadSplitted[2]);
						keystr.append(":");
						valstr.append(msgReadSplitted[3]);
						valstr.append(":");
					}

					msgin.close();
					msgout.close();
					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "OnCreateKey operation UnknownHostException");
				} catch (SocketTimeoutException ste) {
					Log.e(TAG, "OnCreateKey operation SocketTimeoutException");
				} catch (IOException e) {
					Log.e(TAG, "OnCreateKey operation IOException");
				}
				portToSend = portToSend + 2;

			}

			//Insert key value
			if (!keystr.toString().isEmpty()) {
				String[] keySplitted = keystr.toString().split(":");
				String[] valueSplitted = valstr.toString().split(":");
				for (int i = 0; i < keySplitted.length; i++) {
					String eachKey = keySplitted[i];
					String tempCoord = getCoordinator(eachKey);
					String[] tempCoordSplitted = tempCoord.split(",");
					if(Arrays.asList(tempCoordSplitted).contains(myPortID)){
						File filesDir1 = getContext().getFilesDir();
						String[] myfilesNew = filesDir1.list();
						String valueRead = new String();
						StringBuilder stringBuilder = new StringBuilder();
						String eachValue = valueSplitted[i];
						try {
							if (Arrays.asList(myfilesNew).contains(eachKey)) {
								//get the count from the file
								String line;
								FileInputStream fis = getContext().openFileInput(eachKey);
								BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
								while ((line = bufferedReader.readLine()) != null) {
									stringBuilder.append(line);
								}
								valueRead = stringBuilder.toString();
								fis.close();
								String[] valueReadSplitted = valueRead.split("\\.");
								Long countRead = Long.parseLong(valueReadSplitted[1]);
								String[] eachValueSplitted = eachValue.split("\\.");
								Long countReceived = Long.parseLong(eachValueSplitted[1]);

								if (countReceived > countRead) {
									FileOutputStream fos = getContext().openFileOutput(eachKey, Context.MODE_PRIVATE);
									String toWrite = eachValue;
									fos.write(toWrite.getBytes());
									fos.close();
								}

							} else {
								FileOutputStream fos = getContext().openFileOutput(eachKey, Context.MODE_PRIVATE);
								String toWrite = eachValue;
								fos.write(toWrite.getBytes());
								fos.close();
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				}

			}
			return null;
		}
	}
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected synchronized Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try{
				while(true){
					Socket clientSocket = serverSocket.accept();
					DataOutputStream msgout = new DataOutputStream(clientSocket.getOutputStream());
					DataInputStream msgin = new DataInputStream(clientSocket.getInputStream());

					String msgRead = msgin.readUTF();
					String[] msgReadSplitted = msgRead.split(",");
					Context context = getContext();

					//get list of all files
					File filesDir = context.getFilesDir();
					String[] myfiles = filesDir.list();

					// Key insertion
					if(msgReadSplitted[0].equals("0KV")){
						String filename = msgReadSplitted[1];
						String value = msgReadSplitted[2];
						String valueRead = new String();
						StringBuilder stringBuilder = new StringBuilder();
						try {
							if(Arrays.asList(myfiles).contains(filename)){

								//get the count from the file
								String line;
								FileInputStream fis = context.openFileInput(filename);
								BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
								while((line = bufferedReader.readLine())!=null){
									stringBuilder.append(line);
								}
								valueRead = stringBuilder.toString();
								fis.close();
								String[] valueReadSplitted = valueRead.split("\\.");
								Long prevCount = Long.parseLong(valueReadSplitted[1]);
								Long count = System.currentTimeMillis();

								if(count > prevCount){
									FileOutputStream fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
									String toWrite = value + "." + Long.toString(count);
									fos.write(toWrite.getBytes());
									fos.close();
								}

							}
							else{
								FileOutputStream fos = context.openFileOutput(filename, Context.MODE_PRIVATE);
								Long count = System.currentTimeMillis();
								String toWrite = value + "." + count.toString();
								fos.write(toWrite.getBytes());
								fos.close();
							}
						}catch(SocketTimeoutException se){
							Log.e(TAG,"socket timeout in server task");
						}
						catch(Exception e){
							e.printStackTrace();
						}
						msgout.writeUTF("msgrcvd");
						msgin.close();
						msgout.close();
						clientSocket.close();
					}

					// Read operation
					if(msgReadSplitted[0].equals("1Q")){
						String filename = msgReadSplitted[1];
						String value = new String();
						StringBuilder stringBuilder = new StringBuilder();
						if(Arrays.asList(myfiles).contains(filename)){
							try {
								String line;
								FileInputStream fis = context.openFileInput(filename);
								BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
								while((line = bufferedReader.readLine())!=null){
									stringBuilder.append(line);
								}
								value = stringBuilder.toString();
								fis.close();
							} catch (Exception e) {
								Log.e(TAG,"exception in server task query 1Q");
							}
							String msgToSend = "msgrcvd" + "," + value;
							msgout.writeUTF(msgToSend);
							msgin.close();
							msgout.close();
							clientSocket.close();
						}
						else{
							String msgToSend = "msgrcvd" + "," + "empty";
							msgout.writeUTF(msgToSend);
							msgin.close();
							msgout.close();
							clientSocket.close();
						}

					}

					if(msgReadSplitted[0].equals("1Q@")){
						StringBuilder keyString = new StringBuilder();
						StringBuilder valueString = new StringBuilder();
							for(String tempKey : myfiles){
								int comp = tempKey.compareTo("OnCreateFile");
								if(comp!=0){
									keyString.append(tempKey);
									keyString.append(":");
									StringBuilder stringBuilder = new StringBuilder();
									try {
										String line;
										FileInputStream fis = context.openFileInput(tempKey);
										BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
										while((line = bufferedReader.readLine())!=null){
											stringBuilder.append(line);
										}
										String value = new String();
										value = stringBuilder.toString();
										String[] originalValue = value.split("\\.");
										valueString.append(originalValue[0]);
										valueString.append(":");
										fis.close();
									} catch (Exception e) {
										e.printStackTrace();
									}
								}

							}
							String msgToSend = "msgrcvd" + "," + keyString.toString() + "," + valueString.toString();
							msgout.writeUTF(msgToSend);
							msgin.close();
							msgout.close();
							clientSocket.close();

					}

					if(msgReadSplitted[0].equals("2FRQ")){

						if((myfiles!=null)&&(myfiles.length>0)){
							StringBuilder keyString = new StringBuilder();
							StringBuilder valueString = new StringBuilder();
							for(String tempKey : myfiles){
								keyString.append(tempKey);
								keyString.append(":");
								StringBuilder stringBuilder = new StringBuilder();
								try {
									String line;
									FileInputStream fis = context.openFileInput(tempKey);
									BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
									while((line = bufferedReader.readLine())!=null){
										stringBuilder.append(line);
									}
									String value = stringBuilder.toString();
									valueString.append(value);
									valueString.append(":");
									fis.close();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							String msgToSend = "msgrcvd" + "," + "Non" + "," + keyString.toString() + "," + valueString.toString();
							msgout.writeUTF(msgToSend);
							msgin.close();
							msgout.close();
							clientSocket.close();
					}
						else{
							String msgToSend = "msgrcvd" + "," + "empty";
							msgout.writeUTF(msgToSend);
							msgin.close();
							msgout.close();
							clientSocket.close();
						}

					}

					if(msgReadSplitted[0].equals("3D")){
						File filesDirD = getContext().getFilesDir();
						File fileToDelete = new File(filesDirD,msgReadSplitted[1]);
						fileToDelete.delete();
						String msgToSend = "msgrcvd";
						msgout.writeUTF(msgToSend);
						msgin.close();
						msgout.close();
						clientSocket.close();
					}
				}
			}
			catch(IOException x){
				Log.e(TAG, "File read failed");
			}
			return null;
		}
	}
}
