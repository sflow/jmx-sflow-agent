package com.sflow.jmx;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharacterCodingException;
import java.nio.CharBuffer;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.MemoryManagerMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.management.MemoryMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.CompilationMXBean;
import java.lang.management.MemoryUsage;
import java.lang.instrument.Instrumentation;

public class SFlowAgent extends Thread {
    private static final String DEFAULT_CONFIG_FILE = "/etc/hsflowd.auto";

    static String configFile = DEFAULT_CONFIG_FILE;
    private static long lastConfigFileChange = 0L;

    private static final byte[] EMPTY = {};

    private static final int DEFAULT_SFLOW_PORT      = 6343;
    private static final int DEFAULT_DS_INDEX        = 200000;
    private static final int OS_NAME_JAVA            = 13;
    private static final int MACHINE_TYPE_UNKNOWN    = 0;
    private static final int DS_CLASS_PHYSICAL       = 2;
    private static final int DS_CLASS_LOGICAL        = 3;
    private static final int VIR_DOMAIN_RUNNING      = 1;

    public static UUID myUUID        = null;
    public static String myHostname  = null;

    private static int dsIndex       = -1;
    private static int parentDsIndex = -1;

    public static void configFile(String fn) {
	configFile = fn;
	lastConfigFileChange = 0L;
    }
    public static String configFile() { return configFile; }

    public static void uuid(UUID uuid) { myUUID = uuid; };
    public static UUID uuid() {
      if(myUUID != null) return myUUID;

      String uuidStr = System.getProperty("sflow.uuid");
      if(uuidStr != null) {
        try { myUUID = UUID.fromString(uuidStr); }
        catch(IllegalArgumentException e) { ; }
      }
      if(myUUID != null) return myUUID;

      myUUID = new UUID(0L,0L); 
      return myUUID;
    }

    public static void hostname(String hostname) { myHostname = hostname; }
    public static String hostname() {
      if(myHostname != null) return myHostname;

      myHostname = System.getProperty("sflow.hostname");
      if(myHostname != null) return myHostname;

      RuntimeMXBean runtimeMX = ManagementFactory.getRuntimeMXBean();
      myHostname = runtimeMX.getName();
      return myHostname;
    }

    public static void dsIndex(int idx) { dsIndex = idx; }
    public static int dsIndex() {
      if(dsIndex > 0) return dsIndex;

      String indexStr = System.getProperty("sflow.dsindex");
      if(indexStr != null) {
        try { dsIndex = Integer.parseInt(indexStr); }
        catch(NumberFormatException e) { ; }
      }
      if(dsIndex > 0) return dsIndex;

      dsIndex = DEFAULT_DS_INDEX;
      return dsIndex;
    }

    private int agentSequenceNo = 0;
    private int counterSequenceNo = 0;

    private DatagramSocket socket = null;

    private long pollingInterval = 0L;
    private byte[] agentAddress = null;
    private ArrayList<InetSocketAddress> destinations;

    // update configuration
    private void updateConfig() {
	File file = new File(configFile);
	if(!file.exists()) {
            pollingInterval = 0L;
            return;
        }

	long modified = file.lastModified();
	if(modified == lastConfigFileChange) return;

	lastConfigFileChange = modified;

	String rev_start = null;
	String rev_end   = null;
	String sampling  = null;
	String polling   = null;
	String agentIP   = null;
        String parent    = null;
	ArrayList<String> collectors = null;
	try {
	    BufferedReader br = new BufferedReader(new FileReader(file));
	    try {
		String line;
		while((line = br.readLine()) != null) {
		    if(line.startsWith("#")) continue;
		    int idx = line.indexOf('=');
		    if(idx < 0) continue;
		    String key = line.substring(0,idx).trim();
		    String value = line.substring(idx + 1).trim();
		    if("rev_start".equals(key)) rev_start = value;
		    else if("polling".equals(key)) polling = value;
		    else if("agentIP".equals(key)) agentIP = value;
                    else if("ds_index".equals(key)) parent = value;
		    else if("collector".equals(key)) {
			if(collectors == null) collectors = new ArrayList<String>();
			collectors.add(value);
		    }
		    else if("rev_end".equals(key)) rev_end = value;
		}
	    } finally { br.close(); }
	} catch (IOException e) {}

	if(rev_start != null && rev_start.equals(rev_end)) {
	    lastConfigFileChange = modified;

	    // set polling interval
	    if(polling != null) {
		try {
		    long seconds = Long.parseLong(polling);
		    pollingInterval = seconds * 1000L;
		} catch(NumberFormatException e) {
		    pollingInterval = 0L;
		};
	    }
	    else pollingInterval = 0L;

	    // set agent address
	    if(agentIP != null) agentAddress = addressToBytes(agentIP);
	    else agentAddress = null;

            // set parent
            if(parent != null) {
               try { parentDsIndex = Integer.parseInt(parent); }
               catch(NumberFormatException e) {
                  parentDsIndex = -1;
               }
            }
            else parentDsIndex = -1;

	    // set collectors
	    if(collectors != null) {
		ArrayList<InetSocketAddress> newSockets = new ArrayList<InetSocketAddress>();
		for(String socketStr : collectors) {
		    String[] parts = socketStr.split(" ");
		    InetAddress addr = null;
		    try { addr = InetAddress.getByName(parts[0]); }
		    catch(UnknownHostException e) {}
		    if(addr != null) {
			int port = DEFAULT_SFLOW_PORT;
			if(parts.length == 2) {
			    try { port = Integer.parseInt(parts[1]); }
			    catch(NumberFormatException e) {};
			}
			newSockets.add(new InetSocketAddress(addr,port));
		    }
		}
		destinations = newSockets;
	    }
	    else destinations = null;
	}
    }

    // XDR utilty functions

    public static int pad(int len) { return (4 - len) & 3; }

    public static int xdrInt(byte[] buf,int offset,int val) {
	int i = offset;
	buf[i++] = (byte)(val >>> 24);
	buf[i++] = (byte)(val >>> 16);
	buf[i++] = (byte)(val >>> 8);
	buf[i++] = (byte)val;
	return i;
    }

    public static int xdrLong(byte[] buf, int offset, long val) {
	int i = offset;
	buf[i++] = (byte)(val >>> 56);
	buf[i++] = (byte)(val >>> 48);
	buf[i++] = (byte)(val >>> 40);
	buf[i++] = (byte)(val >>> 32);
	buf[i++] = (byte)(val >>> 24);
	buf[i++] = (byte)(val >>> 16);
	buf[i++] = (byte)(val >>> 8);
	buf[i++] = (byte)val;
	return i;
    }

    public static byte[] stringToBytes(String string, int maxLen) {
	CharsetEncoder enc = Charset.forName("US-ASCII").newEncoder();
	enc.onMalformedInput(CodingErrorAction.REPORT);
	enc.onUnmappableCharacter(CodingErrorAction.REPLACE);
	byte[] bytes = null;
	try { bytes = enc.encode(CharBuffer.wrap(string)).array();}
	catch(CharacterCodingException e) { ; }

	if(bytes != null && maxLen > 0 && bytes.length > maxLen) {
	    byte[] original = bytes;
	    bytes = new byte[maxLen];
	    System.arraycopy(original,0,bytes,0,maxLen);
	}
	return bytes;
    }


    public static int xdrBytes(byte[] buf, int offset, byte[] val, int pad, boolean varLen) {
	int i = offset;
	if(varLen) i = xdrInt(buf,i,val.length);
	System.arraycopy(val,0,buf,i,val.length);
	i+=val.length;
	for(int j = 0; j < pad; j++) buf[i++] = 0;
	return i;
    }

    public static int xdrBytes(byte[] buf, int offset, byte[] val, int pad) {
	return xdrBytes(buf,offset,val,pad,false);
    }

    public static int xdrBytes(byte[] buf, int offset, byte[] val) {
	return xdrBytes(buf,offset,val,0);
    }

    public static int xdrString(byte[] buf, int offset, String str, int maxLen) {
        byte[] bytes = str != null ? stringToBytes(str,maxLen) : EMPTY;
        int pad = pad(bytes.length);
        return xdrBytes(buf,offset,bytes,pad,true);
    }

    public static int xdrDatasource(byte[] buf, int offset, int ds_class, int ds_index) {
	int i = offset;

	buf[i++] = (byte)ds_class;
	buf[i++] = (byte)(ds_index >>> 16);
	buf[i++] = (byte)(ds_index >>> 8);
	buf[i++] = (byte)ds_index;

	return i;
    }

    public static int xdrUUID(byte[] buf, int offset, UUID uuid) {
	int i = offset;

	i = xdrLong(buf,i,uuid.getMostSignificantBits());
	i = xdrLong(buf,i,uuid.getLeastSignificantBits());  

	return i;
    }

    private static byte[] addressToBytes(String address) {
	if(address == null) return null;

	byte[] bytes = null;
	try {
	    InetAddress addr = InetAddress.getByName(address);
	    if(addr != null) bytes = addr.getAddress();
	} catch(UnknownHostException e) {
	    bytes = null;
	}
	return bytes;
    }

    // send sFlow datagram
    private void sendDatagram(byte[] datagram, int len) {
	if(socket == null) return;

	for (InetSocketAddress dest : destinations) {
	    try {
		DatagramPacket packet = new DatagramPacket(datagram,len,dest);
		socket.send(packet);
	    } catch(IOException e) {}
	}
    }

    // create sFlow datagram

    // maximum length needed to accomodate IPv6 agent address
    static final int header_len = 36;
    private int xdrSFlowHeader(byte[] buf, int offset) {
	int i = offset;

	RuntimeMXBean runtimeMX = ManagementFactory.getRuntimeMXBean();

	int addrType = agentAddress.length == 4 ? 1 : 2;
	i = xdrInt(buf,i,5);
	i = xdrInt(buf,i,addrType);
	i = xdrBytes(buf,i,agentAddress,pad(agentAddress.length));
	i = xdrInt(buf,i,dsIndex());
	i = xdrInt(buf,i,agentSequenceNo++);
	i = xdrInt(buf,i,(int)runtimeMX.getUptime());

	return i;
    }

    private long totalThreadTime = 0L;
    private HashMap<Long,Long> prevThreadCpuTime = null;
    static final int counter_data_len = 512;
    private int xdrCounterSample(byte[] buf, int offset) {
	int i = offset;

	RuntimeMXBean runtimeMX = ManagementFactory.getRuntimeMXBean();

	String hostname = hostname();

	UUID uuid = uuid();

	String os_release = System.getProperty("java.version");
	String vm_name = runtimeMX.getVmName();
	String vm_vendor = runtimeMX.getVmVendor();
	String vm_version = runtimeMX.getVmVersion();

	List<GarbageCollectorMXBean> gcMXList = ManagementFactory.getGarbageCollectorMXBeans();
        long gcCount = 0;
        long gcTime = 0;
        for(GarbageCollectorMXBean gcMX : gcMXList)  {
          gcCount += gcMX.getCollectionCount();
          gcTime += gcMX.getCollectionTime();
        }

	CompilationMXBean compilationMX = ManagementFactory.getCompilationMXBean();
	long compilationTime = 0L;
        if(compilationMX.isCompilationTimeMonitoringSupported()) {
          compilationTime = compilationMX.getTotalCompilationTime();
        }

	long cpuTime = 0L;
	ThreadMXBean threadMX = ManagementFactory.getThreadMXBean();
	OperatingSystemMXBean osMX = ManagementFactory.getOperatingSystemMXBean();
        String className = osMX.getClass().getName();
        if("com.sun.management.OperatingSystem".equals(className)
           || "com.sun.management.UnixOperatingSystem".equals(className)) {
	    cpuTime = ((com.sun.management.OperatingSystemMXBean)osMX).getProcessCpuTime();
	    cpuTime /= 1000000L;
	} else {
	    if(threadMX.isThreadCpuTimeEnabled()) {
		long[] ids = threadMX.getAllThreadIds();
		HashMap<Long,Long> threadCpuTime = new HashMap<Long,Long>();
		for(int t = 0; t < ids.length; t++) {
		    long id = ids[t];
		    if(id >= 0) {
			long threadtime = threadMX.getThreadCpuTime(id);
			if(threadtime >= 0) {
			    threadCpuTime.put(id,threadtime);
			    long prev = 0L;
			    if(prevThreadCpuTime != null) {
				Long prevl = prevThreadCpuTime.get(id);
				if(prevl != null) prev = prevl.longValue();
			    }
			    if(prev <= threadtime) totalThreadTime += (threadtime - prev);
			    else totalThreadTime += threadtime;
			}
		    }
		}
		cpuTime = (totalThreadTime / 1000000L) + gcTime + compilationTime;
		prevThreadCpuTime = threadCpuTime;
	    }
	}

	MemoryMXBean memoryMX = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory =  memoryMX.getHeapMemoryUsage();
	MemoryUsage nonHeapMemory = memoryMX.getNonHeapMemoryUsage();
   
	int nrVirtCpu = osMX.getAvailableProcessors();

	long memory = heapMemory.getCommitted() + nonHeapMemory.getCommitted();
	long maxMemory = heapMemory.getMax() + nonHeapMemory.getCommitted(); 

	ClassLoadingMXBean classLoadingMX = ManagementFactory.getClassLoadingMXBean();

        long fd_open_count = 0L;
	long fd_max_count = 0L;
        if("com.sun.management.UnixOperatingSystem".equals(className)) {
           fd_open_count = ((com.sun.management.UnixOperatingSystemMXBean)osMX).getOpenFileDescriptorCount();
           fd_max_count = ((com.sun.management.UnixOperatingSystemMXBean)osMX).getMaxFileDescriptorCount();
        }

	// sample_type = counter_sample
	i = xdrInt(buf,i,2);
        int sample_len_idx = i;
        i += 4;
	i = xdrInt(buf,i,counterSequenceNo++);
	i = xdrDatasource(buf,i,DS_CLASS_LOGICAL,dsIndex());
        int sample_nrecs_idx = i;
        int sample_nrecs = 0;
        i += 4;

	// host_descr
	i = xdrInt(buf,i,2000);
        int opaque_len_idx = i;
        i += 4;
	i = xdrString(buf,i,hostname,64);
	i = xdrUUID(buf,i,uuid);
	i = xdrInt(buf,i,MACHINE_TYPE_UNKNOWN);
	i = xdrInt(buf,i,OS_NAME_JAVA);
	i = xdrString(buf,i,os_release,32);
        xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
        sample_nrecs++;

	// host_adapters
	// i = xdrInt(buf,i,2001)
	// Java does not create virtual network interfaces
	// Note: sFlow sub-agent on host OS reports host network adapters

        if(parentDsIndex > 0) {
	  // host_parent
	  i = xdrInt(buf,i,2002);
          opaque_len_idx = i;
          i += 4;
	  i = xdrInt(buf,i,DS_CLASS_PHYSICAL);
	  i = xdrInt(buf,i,parentDsIndex);
          xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
          sample_nrecs++;
        }

	// virt_cpu
	i = xdrInt(buf,i,2101);
        opaque_len_idx = i;
        i += 4; 
	i = xdrInt(buf,i,VIR_DOMAIN_RUNNING);
	i = xdrInt(buf,i,(int)cpuTime);
	i = xdrInt(buf,i,nrVirtCpu);
        xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
        sample_nrecs++;
 
	// virt_memory
	i = xdrInt(buf,i,2102);
        opaque_len_idx = i;
        i += 4;
	i = xdrLong(buf,i,memory);
	i = xdrLong(buf,i,maxMemory);
        xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
        sample_nrecs++;

	// virt_disk_io
	// i = xdrInt(buf,i,2103);
	// Currently no JMX bean providing JVM disk I/O stats
	// Note: sFlow sub-agent on host OS provides overall disk I/O stats

	// virt_net_io
	// i = xdrInt(buf,i,2104);
	// Currently no JMX bean providing JVM network I/O stats
	// Note: sFlow sub-agent on host OS provides overall network I/O stats

	// jvm_runtime
	i = xdrInt(buf,i,2105);
        opaque_len_idx = i;
        i += 4;
	i = xdrString(buf,i,vm_name,64);
	i = xdrString(buf,i,vm_vendor,32);
	i = xdrString(buf,i,vm_version,32);
        xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
        sample_nrecs++;

	// jvm_statistics
	i = xdrInt(buf,i,2106);
        opaque_len_idx = i;
        i += 4;
	i = xdrLong(buf,i,heapMemory.getInit());
	i = xdrLong(buf,i,heapMemory.getUsed());
	i = xdrLong(buf,i,heapMemory.getCommitted());
	i = xdrLong(buf,i,heapMemory.getMax());
	i = xdrLong(buf,i,nonHeapMemory.getInit());
	i = xdrLong(buf,i,nonHeapMemory.getUsed());
	i = xdrLong(buf,i,nonHeapMemory.getCommitted());
	i = xdrLong(buf,i,nonHeapMemory.getMax());
	i = xdrInt(buf,i,(int)gcCount);
	i = xdrInt(buf,i,(int)gcTime);
	i = xdrInt(buf,i,classLoadingMX.getLoadedClassCount());
	i = xdrInt(buf,i,(int)classLoadingMX.getTotalLoadedClassCount());
	i = xdrInt(buf,i,(int)classLoadingMX.getUnloadedClassCount());
	i = xdrInt(buf,i,(int)compilationTime);
	i = xdrInt(buf,i,threadMX.getThreadCount());
	i = xdrInt(buf,i,threadMX.getDaemonThreadCount());
	i = xdrInt(buf,i,(int)threadMX.getTotalStartedThreadCount());
        i = xdrInt(buf,i,(int)fd_open_count);
        i = xdrInt(buf,i,(int)fd_max_count);
        xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
        sample_nrecs++;

        // fill in sample length and record count
        xdrInt(buf,sample_len_idx, i - sample_len_idx - 4);
        xdrInt(buf,sample_nrecs_idx, sample_nrecs);
 
	return i;
    }

    public void pollCounters(long now) {
	if(agentAddress == null) return;

	byte[] buf = new byte[header_len + 4 + counter_data_len];

	int i = 0;

	i = xdrSFlowHeader(buf,i);
	i = xdrInt(buf,i,1);
	i = xdrCounterSample(buf,i);

	sendDatagram(buf,i);
    }

    private long lastPollCounters = 0L;

    public void run() {

	try { socket = new DatagramSocket(); }
	catch(SocketException e) {}

	while(running) {
	    updateConfig();
	    if(pollingInterval > 0L) {
		long now = System.currentTimeMillis();
		if((now - lastPollCounters) > pollingInterval) {
		    pollCounters(now);
                    lastPollCounters = now;
		}
	    }

	    try { sleep(10000); }
	    catch(InterruptedException e) {};
	} 

	if(socket != null) socket.close();
    } 

    private static SFlowAgent task = null;
    private static boolean running = false; 

    public static synchronized void startAgentTask() {
	if(running) return;

	running = true;
	task = new SFlowAgent();
        task.setDaemon(true);
        task.setName("sFlow JVM Thread");
	task.setPriority(Thread.MIN_PRIORITY);
	task.start();
    }  

    public static synchronized void stopAgentTask() {
	if(!running) return;
	
	running = false;
	task.interrupt();
	try { task.join(); }
	catch(InterruptedException e) {};

	task = null;
    }

    public static void premain(String agentArgs, Instrumentation inst) {
       SFlowAgent.startAgentTask();
    }
}
