package it.unibo.scotece.domenico.services.impl;

import it.unibo.scotece.domenico.services.SocketSupport;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SocketSupportImpl implements SocketSupport {

    public static final int BUFFER_SIZE = 1024 * 50;
    private byte[] buffer;
    private Map<String, Double> probability;

    public SocketSupportImpl(){
        this.buffer = new byte[BUFFER_SIZE];
    }

    public SocketSupportImpl(HashMap<String, Double> probability){
        this.buffer = new byte[BUFFER_SIZE];
        this.probability = probability;
    }

    @Override
    public void startServer() throws IOException {
        ServerSocket socket = new ServerSocket(9000);
        Socket client = socket.accept();

        //Get number of files
        DataInputStream din = new DataInputStream(client.getInputStream());

        for (int i = 0; i < din.readInt(); i++){

            /*BufferedInputStream in =
                    new BufferedInputStream(client.getInputStream());*/

            var in = new DataInputStream(client.getInputStream());
            String fileName = in.readUTF();

            var backup =  Paths.get("").toAbsolutePath().normalize().toString() + fileName + ".archive";

            BufferedOutputStream out =
                    new BufferedOutputStream(new FileOutputStream(backup));

            int len = 0;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
                System.out.print("#");
            }

            in.close();
            out.flush();
            out.close();

        }

        client.close();
        socket.close();
        System.out.println("\nDone!");

    }

    @Override
    public void startClient(String address) throws Exception {

        Socket socket = new Socket(address, 9000);

        //Send number of files
        DataOutputStream files = new DataOutputStream(socket.getOutputStream());
        files.writeInt(this.probability.size());

        for (var p : this.probability.entrySet()){
            var backup =  Paths.get("").toAbsolutePath().normalize().toString() + p.getKey() + ".archive";

            BufferedInputStream in =
                    new BufferedInputStream(
                            new FileInputStream(backup));

            BufferedOutputStream out =
                    new BufferedOutputStream(socket.getOutputStream());


            int len = 0;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
                System.out.print("#");
            }

            in.close();
            out.flush();
            out.close();

        }

        socket.close();
        System.out.println("\nDone!");

    }
}
