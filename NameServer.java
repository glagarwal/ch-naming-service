import java.net.*;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;

class NameServer implements Runnable {
    public static final Boolean DEBUG = false;
    public static final Boolean ShowException = false;
    public static TreeMap<Integer, String> pairsMap = new TreeMap<Integer, String>();
    public static int serverId;
    public static int portNumber;
    public DataInputStream receive;
    public DataOutputStream send;
    public ServerSocket ns;

    public static String pre_nsAddr, succ_nsAddr, bsAddr;
    public static int pre_nsID, succ_nsID;
    public static int pre_nsPort, succ_nsPort, bsPort;

    public void addServer(Socket s) {
        try {
            this.receive = new DataInputStream(s.getInputStream());
            this.send = new DataOutputStream(s.getOutputStream());
            String nsAddr = s.getInetAddress().getHostAddress();
            int nsID = this.receive.readInt();
            int nsPort = this.receive.readInt();

            if (serverId < nsID) {
                this.send.writeUTF("go ahead");
                this.send.writeUTF(succ_nsAddr + ":" + succ_nsID + ":" + succ_nsPort);
                return;
            }
            int key;
            ArrayList<Integer> removeKeys = new ArrayList<Integer>();
            send.writeUTF("sending pairs");
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
                System.out.println("Sending (" + key + " : " + p.getValue()+")");
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

    public void deleteServer(Socket s) {
        try {
            System.out.print("Predecessor "+pre_nsID+" exited. ");
            pre_nsAddr = this.receive.readUTF();
            pre_nsID = this.receive.readInt();
            pre_nsPort = this.receive.readInt();
            System.out.println("New predecessor "+pre_nsID);
            int key;
            while (true) {
                String repMsg = this.receive.readUTF();
                if (repMsg.equalsIgnoreCase("done")) {
                    break;
                }
                key = Integer.parseInt(repMsg.split(":")[0]);
                pairsMap.put(key, repMsg.split(":")[1]);
                System.out.println("Receiving: ("+repMsg+")");
            }
            System.out.println();
        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }
    }

    public void run() {
        try {
            ns = new ServerSocket(portNumber);
            Socket s;

            // pre_nsAddr = succ_nsAddr = s.getLocalAddress();
            // pre_nsID = succ_nsID = serverId;
            // pre_nsPort = succ_nsPort = portNumber;

            // addServer(s);
            // s.close();

            String repMsg = "";
            while (true) {
                s = ns.accept();
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
                    repMsg = receive.readUTF();
                    // successor is exiting
                    if (repMsg.equalsIgnoreCase("succ")) {
                        System.out.print("Successor "+succ_nsID+" exited. ");
                        succ_nsAddr = this.receive.readUTF();
                        succ_nsID = this.receive.readInt();
                        succ_nsPort = this.receive.readInt();
                        System.out.println("New Successor "+succ_nsID);
                    }
                    // predecesor is exiting
                    else if (repMsg.equalsIgnoreCase("pre")) {
                        deleteServer(s);
                    }
                } else if (repMsg.equalsIgnoreCase("lookup")) {
                    int key = this.receive.readInt();
                    if (pairsMap.containsKey(key)) {
                        this.send.writeUTF("found");
                        this.send.writeUTF(pairsMap.get(key));
                        if (this.receive.readUTF().equalsIgnoreCase("delete key")) {
                            pairsMap.remove(key);
                        }
                    } else {
                        this.send.writeUTF("go ahead");
                        this.send.writeUTF(succ_nsAddr);
                        this.send.writeInt(succ_nsID);
                        this.send.writeInt(succ_nsPort);
                    }
                } else if (repMsg.equalsIgnoreCase("insert")) {
                    int key = this.receive.readInt();
                    String value = this.receive.readUTF();
                    if (key > pre_nsID && key <= serverId) {
                        this.send.writeUTF("found");
                        pairsMap.put(key, value);
                    } else {
                        this.send.writeUTF("go ahead");
                        this.send.writeUTF(succ_nsAddr);
                        this.send.writeInt(succ_nsID);
                        this.send.writeInt(succ_nsPort);
                    }
                }
                s.close();
            }
        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }
    }

    public void displayTraversal() {
        System.out.println("contacted server: " + succ_nsID);
    }

    public void displayRange() {
        System.out.println("Key range: " + pairsMap.firstKey() + " to " + pairsMap.lastKey());
    }

    public void entry() {
        Socket c;
        try {
            // loops through the servers till it finds one.
            while (true) {
                int flag = 0;
                if (DEBUG)
                    System.out.println("successor address: " + succ_nsAddr + ":" + succ_nsPort);
                c = new Socket(succ_nsAddr, succ_nsPort);
                this.receive = new DataInputStream(c.getInputStream());
                this.send = new DataOutputStream(c.getOutputStream());
                this.send.writeUTF("enter");
                this.send.writeInt(serverId);
                this.send.writeInt(portNumber);

                String repMsg = this.receive.readUTF();
                if (repMsg.equalsIgnoreCase("go ahead")) {
                    repMsg = this.receive.readUTF();
                    succ_nsAddr = repMsg.split(":")[0];
                    succ_nsID = Integer.parseInt(repMsg.split(":")[1]);
                    succ_nsPort = Integer.parseInt(repMsg.split(":")[2]);
                    c.close();
                    displayTraversal();
                    continue;
                } else if (repMsg.equalsIgnoreCase("sending pairs")) {
                    int key;
                    // keeps receiving key-values till "done" received
                    while (true) {
                        repMsg = this.receive.readUTF();
                        if (repMsg.equalsIgnoreCase("done")) {
                            repMsg = this.receive.readUTF();
                            pre_nsAddr = repMsg.split(":")[0];
                            pre_nsID = Integer.parseInt(repMsg.split(":")[1]);
                            pre_nsPort = Integer.parseInt(repMsg.split(":")[2]);

                            repMsg = this.receive.readUTF();
                            succ_nsAddr = repMsg.split(":")[0];
                            succ_nsID = Integer.parseInt(repMsg.split(":")[1]);
                            succ_nsPort = Integer.parseInt(repMsg.split(":")[2]);

                            c.close();
                            //informing the predecessor about itself
                            c = new Socket(pre_nsAddr, pre_nsPort);
                            this.receive = new DataInputStream(c.getInputStream());
                            this.send = new DataOutputStream(c.getOutputStream());
                            this.send.writeUTF("inform predecessor");
                            this.send.writeInt(serverId);
                            this.send.writeInt(portNumber);
                            flag = 1;
                            break;
                        }
                        key = Integer.parseInt(repMsg.split(":")[0]);
                        pairsMap.put(key, repMsg.split(":")[1]);
                    }
                }
                if (flag == 1)
                    break;
            }
        } catch (Exception e) {
            if (ShowException) {
                e.printStackTrace();
            }
        }
    }

    public void exit() {
        try {
            //informing the predecessor
            Socket c = new Socket(pre_nsAddr, pre_nsPort);
            this.receive = new DataInputStream(c.getInputStream());
            this.send = new DataOutputStream(c.getOutputStream());
            this.send.writeUTF("exit");
            this.send.writeUTF("succ");
            this.send.writeUTF(succ_nsAddr);
            this.send.writeInt(succ_nsID);
            this.send.writeInt(succ_nsPort);
            c.close();
            // informing the successor and sending key-value pairs
            if (DEBUG)
                System.out.println("Successor " + succ_nsAddr + ":" + succ_nsPort);
            c = new Socket(succ_nsAddr, succ_nsPort);
            this.receive = new DataInputStream(c.getInputStream());
            this.send = new DataOutputStream(c.getOutputStream());
            this.send.writeUTF("exit");
            this.send.writeUTF("pre");
            this.send.writeUTF(pre_nsAddr);
            this.send.writeInt(pre_nsID);
            this.send.writeInt(pre_nsPort);
            int key;
            for (Entry p : pairsMap.entrySet()) {
                key = (int) p.getKey();
                this.send.writeUTF(key + ":" + p.getValue());
            }
            this.send.writeUTF("done");

        } catch (Exception e) {
            if (ShowException)
                e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            File file = new File(args[0]);
            Scanner sc = new Scanner(file);
            serverId = Integer.parseInt(sc.nextLine());
            portNumber = Integer.parseInt(sc.nextLine());
            String bootstrap[] = sc.nextLine().split(" ");
            bsAddr = bootstrap[0];
            bsPort = Integer.parseInt(bootstrap[1]);
            NameServer n = new NameServer();

            Thread t = new Thread(new NameServer());
            t.start();
            String cmd = "";
            Scanner input = new Scanner(System.in);
            while (!cmd.equalsIgnoreCase("exit")) {
                System.out.print("\nCommand> ");
                cmd = input.next();
                if (cmd.equalsIgnoreCase("enter")) {
                    succ_nsAddr = bsAddr;
                    succ_nsPort = bsPort;
                    n.displayTraversal();
                    n.entry();
                    n.displayRange();
                    System.out.println("Predecessor ID: " + pre_nsID);
                    System.out.println("Successor ID: " + succ_nsID);
                } else if (cmd.equalsIgnoreCase("exit")) {
                    // Socket c = new Socket(bsAddr, bsPort);
                    n.exit();
                    n.displayRange();
                    System.out.println("Successor ID: " + succ_nsID);
                    System.out.println("Successful exit! GoodBye.");
                    n.ns.close();
                } else {
                    System.out.println("Please input \"enter\" or \"exit\" commands only.");
                }
            }
        } catch (Exception e) {
            if (ShowException) {
                System.out.println("UNEXPECTED_ERROR: ");
                e.printStackTrace();
            }
            System.exit(0);
        }
    }
}