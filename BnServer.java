import java.net.*;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class BnServer implements Runnable {
    public static final Boolean DEBUG = false;
    public static final Boolean ShowException = false;
    public static TreeMap<Integer, String> pairsMap = new TreeMap<Integer, String>();
    // public static Map<Integer, String> nameServers = new TreeMap<Integer, String>();
    public static int serverId;
    public static int portNumber;
    public DataInputStream receive;
    public DataOutputStream send;
    public ServerSocket bn;

    public static String pre_nsAddr, succ_nsAddr;
    public static int pre_nsID, succ_nsID;
    public static int pre_nsPort, succ_nsPort;

    /**
     * This will add the server to the correct position
     * and will assign the key range.
     * 
     * @author Gaurav Agarwal
     * @param s The socket connection to the Name Server
     */
    public void addServer(Socket s) {
        try {
            String nsAddr = s.getInetAddress().getHostAddress();
            int nsID = this.receive.readInt();
            int nsPort = this.receive.readInt();
            // this gives the next server
            if (pairsMap.firstKey() > nsID) {
                if (DEBUG)
                    System.out.println("Going ahead to " + succ_nsID);
                this.send.writeUTF("go ahead");
                this.send.writeUTF(succ_nsAddr + ":" + succ_nsID + ":" + succ_nsPort);
                return;
            }

            int key;
            ArrayList<Integer> removeKeys = new ArrayList<Integer>();
            this.send.writeUTF("sending pairs");
            for (Entry p : pairsMap.entrySet()) {
                key = (int) p.getKey();
                if (key > nsID) {
                    this.send.writeUTF("done");
                    // sends it's predecessor's info
                    this.send.writeUTF(pre_nsAddr + ":" + pre_nsID + ":" + pre_nsPort);
                    // informs about self as being the successor
                    this.send.writeUTF(s.getLocalAddress().getHostAddress() + ":" + serverId + ":" + portNumber);
                    pre_nsAddr = nsAddr;
                    pre_nsID = nsID;
                    pre_nsPort = nsPort;
                    break;
                }
                this.send.writeUTF(key + ":" + p.getValue());
                System.out.println("Sending (" + key + " : " + p.getValue() + ")");
                removeKeys.add(key);
            }
            System.out.println();
            //removing the keys that now belong to the new server
            for (int i = 0; i < removeKeys.size(); i++) {
                pairsMap.remove(removeKeys.get(i));
            }
        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }
    }

    /**
     * It reassigns it's own predecessor and
     * takes all the key-value pairs of the exiting server.
     * 
     * @author Gaurav Agarwal
     * @param s The Socket connection to the exiting name server.
     */
    public void deleteServer(Socket s) {
        try {
            System.out.print("Predecessor " + pre_nsID + " exited. ");
            pre_nsAddr = this.receive.readUTF();
            pre_nsID = this.receive.readInt();
            pre_nsPort = this.receive.readInt();
            System.out.println("New predecessor " + pre_nsID);
            int key;
            while (true) {
                String repMsg = this.receive.readUTF();
                if (repMsg.equalsIgnoreCase("done")) {
                    break;
                }
                key = Integer.parseInt(repMsg.split(":")[0]);
                pairsMap.put(key, repMsg.split(":")[1]);
                System.out.println("Receiving: (" + repMsg + ")");
            }
            System.out.println();
        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }
    }

    /**
     * A thread run method which will always wait 
     * to accept incoming connections. On connection,
     * it performs the requested task.
     * 
     * @author Gaurav Agarwal
     */
    public void run() {
        try {
            bn = new ServerSocket(portNumber);
            Socket s = bn.accept();
            this.receive = new DataInputStream(s.getInputStream());
            this.send = new DataOutputStream(s.getOutputStream());

            pre_nsAddr = succ_nsAddr = s.getLocalAddress().getHostAddress();
            pre_nsID = succ_nsID = serverId;
            pre_nsPort = succ_nsPort = portNumber;
            String repMsg = this.receive.readUTF();
            addServer(s);
            s.close();

            while (true) {
                s = bn.accept();
                this.receive = new DataInputStream(s.getInputStream());
                this.send = new DataOutputStream(s.getOutputStream());

                repMsg = this.receive.readUTF();
                if (repMsg.equalsIgnoreCase("inform predecessor")) {
                    succ_nsAddr = s.getInetAddress().getHostAddress();
                    succ_nsID = this.receive.readInt();
                    succ_nsPort = this.receive.readInt();
                } else if (repMsg.equalsIgnoreCase("enter")) {
                    addServer(s);
                } else if (repMsg.equalsIgnoreCase("exit")) {
                    repMsg = this.receive.readUTF();
                    // successor is exiting
                    if (repMsg.equalsIgnoreCase("succ")) {
                        System.out.print("Successor " + succ_nsID + " exited. ");
                        succ_nsAddr = this.receive.readUTF();
                        succ_nsID = this.receive.readInt();
                        succ_nsPort = this.receive.readInt();
                        System.out.println("New Successor " + succ_nsID);
                    }
                    // predecesor is exiting
                    else if (repMsg.equalsIgnoreCase("pre")) {
                        deleteServer(s);
                    }
                }
                s.close();
            }
        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }

    }

    /**
     * This contacts the name serevers to check
     * whether they have the key-value pair.
     * This stops when all the servers till the server 
     * having the search key's range are evaluated.
     * Alongside lookup if there is a delete key request,
     * it performs that too.
     *
     * @author Gaurav Agaarwal
     * @param key The key whose value is to be found
     * @param task the task to perform such as just lookup or delete key is specified here.
     * @return This returns a string containing the value of that key or else "Key not found".
     */
    public String lookUp(int key, String task) {
        String val = "Key not found";
        try {
            String nsAddr = succ_nsAddr;
            int nsID = succ_nsID;
            int nsPort = succ_nsPort;
            System.out.println("Searching at Server " + serverId);
            if (key > pre_nsID) {
                if (pairsMap.containsKey(key)) {
                    System.out.println("Key found at Bootstrap server!");
                    val = pairsMap.get(key);
                    if (task.equalsIgnoreCase("delete key")) {
                        pairsMap.remove(key);
                        System.out.print("Succesful deletion ");
                    }
                }
            } else {
                while (nsID != serverId) {
                    int flag = 0;
                    System.out.println("Contacting Server " + nsID);
                    Socket s = new Socket(nsAddr, nsPort);
                    this.receive = new DataInputStream(s.getInputStream());
                    this.send = new DataOutputStream(s.getOutputStream());
                    String rep = "";
                    send.writeUTF("lookup");
                    send.writeInt(key);
                    rep = receive.readUTF();
                    if (rep.equalsIgnoreCase("go ahead")) {
                        if (key < nsID) { //it should have been here, don't check further
                            flag = 1;
                        }
                        nsAddr = this.receive.readUTF();
                        nsID = this.receive.readInt();
                        nsPort = this.receive.readInt();
                    } else if (rep.equalsIgnoreCase("found")) {
                        System.out.println("Key found at Server " + nsID);
                        val = this.receive.readUTF();
                        if (task.equalsIgnoreCase("delete key")) {
                            this.send.writeUTF(task);
                            System.out.print("Succesful deletion ");
                        } else {
                            this.send.writeUTF("okay");
                        }
                        break;
                    }
                    s.close();
                    if (flag == 1)
                        break;
                }
            }
        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }
        return val;
    }

    /**
     * This contacts all the name serevers 
     * until the name server having the key range is found.
     *
     * @author Gaurav Agaarwal
     * @param key
     * @param value 
     */
    public void insert(int key, String value) {
        try {
            String nsAddr = succ_nsAddr;
            int nsID = succ_nsID;
            int nsPort = succ_nsPort;
            System.out.println("Starting at Server " + serverId);
            if (key > pre_nsID) {
                pairsMap.put(key, value);
                System.out.print("Inserted at Server " + serverId);
            } else {
                while (nsID != serverId) {
                    System.out.println("Contacting Server " + nsID);
                    Socket s = new Socket(nsAddr, nsPort);
                    this.receive = new DataInputStream(s.getInputStream());
                    this.send = new DataOutputStream(s.getOutputStream());
                    String rep = "";
                    this.send.writeUTF("insert");
                    this.send.writeInt(key);
                    this.send.writeUTF(value);
                    rep = receive.readUTF();
                    if (rep.equalsIgnoreCase("go ahead")) {
                        nsAddr = this.receive.readUTF();
                        nsID = this.receive.readInt();
                        nsPort = this.receive.readInt();
                    } else if (rep.equalsIgnoreCase("found")) {
                        System.out.println("Inserted at Server " + nsID);
                        break;
                    }
                    s.close();
                }
            }
        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }
    }

    /**
     * main method where the TreeMap is filled with initial values
     * And it takes the user input commands.
     * 
     * @author Gaurav Agarwal
     */
    public static void main(String[] args) {
        try {
            File file = new File(args[0]);
            Scanner sc = new Scanner(file);
            serverId = Integer.parseInt(sc.nextLine());
            portNumber = Integer.parseInt(sc.nextLine());
            int key;
            while (sc.hasNextLine()) {
                String[] parts = sc.nextLine().split(" ");
                key = Integer.parseInt(parts[0]);
                pairsMap.put(key, parts[1]);
            }
            if (DEBUG) { //just printing the Treemap key-value pairs
                System.out.println(Arrays.asList(pairsMap));
                System.out.println("\nLength: " + pairsMap.size());
            }
            BnServer b = new BnServer();
            Thread t = new Thread(new BnServer());
            t.start();
            String cmd = "";
            Scanner input = new Scanner(System.in);
            while (!cmd.equalsIgnoreCase("exit")) {
                System.out.print("\nCommand> ");
                cmd = input.nextLine();

                if (cmd.contains("lookup")) {
                    int k = Integer.parseInt(cmd.split(" ")[1]);
                    System.out.println("(" + k + " : " + b.lookUp(k, "lookup") + ")");
                } else if (cmd.contains("insert")) {
                    int k = Integer.parseInt(cmd.split(" ")[1]);
                    String v = cmd.split(" ")[2];
                    b.insert(k, v);
                } else if (cmd.contains("delete")) {
                    int k = Integer.parseInt(cmd.split(" ")[1]);
                    System.out.println("(" + k + " : " + b.lookUp(k, "delete key") + ")");
                }
            }
            b.bn.close();
        } catch (Exception e) {
            if (ShowException)
                System.out.println("UNEXPECTED_ERROR: " + e);
            System.exit(0);
        }
    }
}