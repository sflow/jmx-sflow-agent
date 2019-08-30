package com.sflow.jmx;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.management.MemoryMXBean;
import java.lang.management.CompilationMXBean;
import java.lang.management.MemoryUsage;
import java.lang.instrument.Instrumentation;
import java.lang.management.ClassLoadingMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;

public class SFlowAgent extends Thread {

    static final String CONFIG_FILE = System.getProperty("sflow.config","/etc/hsflowd.auto");
    static final byte[] EMPTY = {};
    static final int DEFAULT_SFLOW_PORT = 6343;
    static final int DEFAULT_DS_INDEX = 200000;
    static final int OS_NAME_JAVA = 13;
    static final int MACHINE_TYPE_UNKNOWN = 0;
    static final int DS_CLASS_PHYSICAL = 2;
    static final int DS_CLASS_LOGICAL = 3;
    static final int VIR_DOMAIN_RUNNING = 1;
    static final int BUF_SZ = 1024;
    static final int SFLOW_VERSION = 5;
    static final int COUNTER_SAMPLE = 2;
    static final long SLEEP_MIN = 1000L;
    static final long SLEEP_MAX = 10000L;

    private int agentSequenceNo = 0;
    private int counterSequenceNo = 0;

    private UUID myUUID = null;
    private UUID uuid() {
        if (myUUID != null) {
            return myUUID;
        }

        String uuidStr = System.getProperty("sflow.uuid");
        if (uuidStr != null) {
            try {
                myUUID = UUID.fromString(uuidStr);
            } catch (IllegalArgumentException e) {
                myUUID = new UUID(0L, 0L);
            }
        } else {
            myUUID = new UUID(0L, 0L);
        }
        return myUUID;
    }

    private String myHostname = null;
    private String hostname() {
        if (myHostname != null) {
            return myHostname;
        }

        myHostname = System.getProperty("sflow.hostname");
        if (myHostname == null) {
            RuntimeMXBean runtimeMX = ManagementFactory.getRuntimeMXBean();
            myHostname = runtimeMX.getName();
        }
        return myHostname;
    }

    private int dsIndex = -1;
    private int dsIndex() {
        if (dsIndex != -1) {
            return dsIndex;
        }

        dsIndex = Integer.getInteger("sflow.dsindex", DEFAULT_DS_INDEX);
        return dsIndex;
    }

    private long lastConfigFileChange = 0L;
    private int parentDsIndex = -1;
    private long pollingInterval = 0L;
    private byte[] agentAddress = null;
    private List<InetSocketAddress> destinations;
    private void updateConfig() {
        File file = new File(CONFIG_FILE);
        if (!file.canRead()) {
            pollingInterval = 0L;
            return;
        }

        long modified = file.lastModified();
        if (modified == lastConfigFileChange) {
            return;
        }

        lastConfigFileChange = modified;

        String rev_start = null;
        String rev_end = null;
        String sampling = null;
        String polling = null;
        String agentIP = null;
        String parent = null;
        List<String> collectors = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                int idx = line.indexOf('=');
                if (idx < 0) {
                    continue;
                }
                String key = line.substring(0, idx).trim();
                String value = line.substring(idx + 1).trim();
                switch (key) {
                    case "rev_start":
                        rev_start = value;
                        break;
                    case "polling":
                        polling = value;
                        break;
                    case "agentIP":
                        agentIP = value;
                        break;
                    case "ds_index":
                        parent = value;
                        break;
                    case "collector":
                        collectors.add(value);
                        break;
                    case "rev_end":
                        rev_end = value;
                        break;
                    default:
                        break;
                }
            }
        } catch (IOException e) {
        }

        if (rev_start != null && rev_start.equals(rev_end)) {
            lastConfigFileChange = modified;

            // set polling interval
            if (polling != null) {
                try {
                    long seconds = Long.parseLong(polling);
                    pollingInterval = seconds * 1000L;
                } catch (NumberFormatException e) {
                    pollingInterval = 0L;
                }
            } else {
                pollingInterval = 0L;
            }

            // set agent address
            if (agentIP != null) {
                try {
                    InetAddress addr = InetAddress.getByName(agentIP);
                    if (addr != null) {
                        agentAddress = addr.getAddress();
                    } else {
                        agentAddress = null;
                    }
                } catch (UnknownHostException e) {
                    agentAddress = null;
                }
            } else {
                agentAddress = null;
            }

            // set parent
            if (parent != null) {
                try {
                    parentDsIndex = Integer.parseInt(parent);
                } catch (NumberFormatException e) {
                    parentDsIndex = -1;
                }
            } else {
                parentDsIndex = -1;
            }

            // set collectors
            if (collectors.size() > 0) {
                List<InetSocketAddress> newSockets = new ArrayList<>();
                for (String socketStr : collectors) {
                    String[] parts = socketStr.split(" ");
                    try {
                        InetAddress addr = InetAddress.getByName(parts[0]);
                        int port = parts.length == 2 ? Integer.parseInt(parts[1]) : DEFAULT_SFLOW_PORT;
                        newSockets.add(new InetSocketAddress(addr, port));
                    } catch (UnknownHostException e) {
                    } catch (NumberFormatException nf) {
                    }
                }
                destinations = newSockets.size() > 0 ? newSockets : null;
            } else {
                destinations = null;
            }
        }
    }

    private void putString(ByteBuffer buf, String str, int maxLen) {
        byte[] bytes = str != null ? str.getBytes(StandardCharsets.US_ASCII) : EMPTY;
        int len = Math.min(maxLen, bytes.length);
        buf.putInt(len);
        buf.put(bytes, 0, len);
        int pad = (4 - len) & 3;
        for (int i = 0; i < pad; i++) {
            buf.put((byte) 0);
        }
    }

    private long valueOrUnknown(long val) {
        return val != -1L ? val : 0L;
    }

    private void takeSample(ByteBuffer buf) {
        UUID uuid = uuid();
        String os_release = System.getProperty("java.version","");

        RuntimeMXBean runtimeMX = ManagementFactory.getRuntimeMXBean();
        String vm_name = runtimeMX.getVmName();
        String vm_vendor = runtimeMX.getVmVendor();
        String vm_version = runtimeMX.getVmVersion();

        List<GarbageCollectorMXBean> gcMXList = ManagementFactory.getGarbageCollectorMXBeans();
        long gcCount = 0;
        long gcTime = 0;
        for (GarbageCollectorMXBean gcMX : gcMXList) {
            gcCount += gcMX.getCollectionCount();
            gcTime += gcMX.getCollectionTime();
        }

        CompilationMXBean compilationMX = ManagementFactory.getCompilationMXBean();
        long compilationTime = 0L;
        if (compilationMX.isCompilationTimeMonitoringSupported()) {
            compilationTime = compilationMX.getTotalCompilationTime();
        }

        ThreadMXBean threadMX = ManagementFactory.getThreadMXBean();
        OperatingSystemMXBean osMX = ManagementFactory.getOperatingSystemMXBean();

        long cpuTime = 0L;
        long fd_open_count = 0L;
        long fd_max_count = 0L;
        int nrVirtCpu = osMX.getAvailableProcessors();
        if(osMX instanceof com.sun.management.OperatingSystemMXBean) {
            cpuTime = ((com.sun.management.OperatingSystemMXBean) osMX).getProcessCpuTime();
            cpuTime /= 1000000L;
            if(osMX instanceof com.sun.management.UnixOperatingSystemMXBean) {
                fd_open_count = ((com.sun.management.UnixOperatingSystemMXBean) osMX).getOpenFileDescriptorCount();
                fd_max_count = ((com.sun.management.UnixOperatingSystemMXBean) osMX).getMaxFileDescriptorCount();
            }
        }

        MemoryMXBean memoryMX = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory = memoryMX.getHeapMemoryUsage();
        MemoryUsage nonHeapMemory = memoryMX.getNonHeapMemoryUsage();

        long memory = heapMemory.getCommitted() + nonHeapMemory.getCommitted();
        long maxMemory = heapMemory.getMax() + nonHeapMemory.getCommitted();

        ClassLoadingMXBean classLoadingMX = ManagementFactory.getClassLoadingMXBean();

        // sFlow Version 5
        // https://sflow.org/sflow_version_5.txt

        // sample_datagram_v5
        int addrType = agentAddress.length == 4 ? 1 : 2;
        buf.putInt(SFLOW_VERSION);
        buf.putInt(addrType);
        buf.put(agentAddress);
        buf.putInt(dsIndex());
        buf.putInt(agentSequenceNo++);
        buf.putInt((int) runtimeMX.getUptime());
        buf.putInt(1);
        buf.putInt(COUNTER_SAMPLE);

        // make space for sample length
        int sample_len_idx = buf.position();
        buf.position(sample_len_idx + 4);

        buf.putInt(counterSequenceNo++);
        buf.putInt(DS_CLASS_LOGICAL << 24 | dsIndex());

        // make space for number of records
        int sample_nrecs_idx = buf.position();
        buf.position(sample_nrecs_idx + 4);
        int sample_nrecs = 0;

        // sFlow Host Structures
        // https://sflow.org/sflow_host.txt

        // host_descr
        buf.putInt(2000);
        int opaque_len_idx = buf.position();
        buf.position(opaque_len_idx + 4);
        putString(buf, hostname(), 64);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        buf.putInt(MACHINE_TYPE_UNKNOWN);
        buf.putInt(OS_NAME_JAVA);
        putString(buf, os_release, 32);
        buf.putInt(opaque_len_idx, buf.position() - opaque_len_idx - 4);
        sample_nrecs++;

        // host_adapters
        // i = xdrInt(buf,i,2001)
        // Java does not create virtual network interfaces
        // Note: sFlow sub-agent on host OS reports host network adapters
        if (parentDsIndex != -1) {
            // host_parent
            buf.putInt(2002);
            opaque_len_idx = buf.position();
            buf.position(opaque_len_idx + 4);
            buf.putInt(DS_CLASS_PHYSICAL);
            buf.putInt(parentDsIndex);
            buf.putInt(opaque_len_idx, buf.position() - opaque_len_idx - 4);
            sample_nrecs++;
        }

        // virt_cpu
        buf.putInt(2101);
        opaque_len_idx = buf.position();
        buf.position(opaque_len_idx + 4);
        buf.putInt(VIR_DOMAIN_RUNNING);
        buf.putInt((int) cpuTime);
        buf.putInt(nrVirtCpu);
        buf.putInt(opaque_len_idx, buf.position() - opaque_len_idx - 4);
        sample_nrecs++;

        // virt_memory
        buf.putInt(2102);
        opaque_len_idx = buf.position();
        buf.position(opaque_len_idx + 4);
        buf.putLong(memory);
        buf.putLong(maxMemory);
        buf.putInt(opaque_len_idx, buf.position() - opaque_len_idx - 4);
        sample_nrecs++;

        // virt_disk_io
        // i = xdrInt(buf,i,2103);
        // Currently no JMX bean providing JVM disk I/O stats
        // Note: sFlow sub-agent on host OS provides overall disk I/O stats

        // virt_net_io
        // i = xdrInt(buf,i,2104);
        // Currently no JMX bean providing JVM network I/O stats
        // Note: sFlow sub-agent on host OS provides overall network I/O stats

        // sFlow Java Virtual Machine Structures
        // https://sflow.org/sflow_jvm.txt

        // jvm_runtime
        buf.putInt(2105);
        opaque_len_idx = buf.position();
        buf.position(opaque_len_idx + 4);
        putString(buf, vm_name, 64);
        putString(buf, vm_vendor, 32);
        putString(buf, vm_version, 32);
        buf.putInt(opaque_len_idx, buf.position() - opaque_len_idx - 4);
        sample_nrecs++;

        // jvm_statistics
        buf.putInt(2106);
        opaque_len_idx = buf.position();
        buf.position(opaque_len_idx + 4);
        buf.putLong(valueOrUnknown(heapMemory.getInit()));
        buf.putLong(heapMemory.getUsed());
        buf.putLong(heapMemory.getCommitted());
        buf.putLong(valueOrUnknown(heapMemory.getMax()));
        buf.putLong(valueOrUnknown(nonHeapMemory.getInit()));
        buf.putLong(nonHeapMemory.getUsed());
        buf.putLong(nonHeapMemory.getCommitted());
        buf.putLong(valueOrUnknown(nonHeapMemory.getMax()));
        buf.putInt((int) gcCount);
        buf.putInt((int) gcTime);
        buf.putInt(classLoadingMX.getLoadedClassCount());
        buf.putInt((int) classLoadingMX.getTotalLoadedClassCount());
        buf.putInt((int) classLoadingMX.getUnloadedClassCount());
        buf.putInt((int) compilationTime);
        buf.putInt(threadMX.getThreadCount());
        buf.putInt(threadMX.getDaemonThreadCount());
        buf.putInt((int) threadMX.getTotalStartedThreadCount());
        buf.putInt((int) fd_open_count);
        buf.putInt((int) fd_max_count);
        buf.putInt(opaque_len_idx, buf.position() - opaque_len_idx - 4);
        sample_nrecs++;

        // fill in sample length and record count
        buf.putInt(sample_len_idx, buf.position() - sample_len_idx - 4);
        buf.putInt(sample_nrecs_idx, sample_nrecs);
    }

    @Override
    public void run() {
        try (DatagramChannel channel = DatagramChannel.open()) {
            long lastPollCounters = 0L;
            ByteBuffer buf = ByteBuffer.allocate(BUF_SZ);
            buf.order(ByteOrder.BIG_ENDIAN);
            while (true) {
                updateConfig();
                if (pollingInterval > 0L && agentAddress != null && destinations != null) {
                    long now = System.currentTimeMillis();
                    if ((now - lastPollCounters) >= pollingInterval) {
                        buf.clear();
                        takeSample(buf);
                        buf.flip();
                        for (InetSocketAddress dest : destinations) {
                            try {
                                channel.send(buf, dest);
                            } catch (IOException e) {
                            }
                            buf.rewind();
                        }
                        lastPollCounters = now;
                    }
                }
                sleep(pollingInterval >= SLEEP_MIN && pollingInterval < SLEEP_MAX ? pollingInterval : SLEEP_MAX);
            }
        } catch (IOException ioe) {
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void premain(String agentArgs, Instrumentation inst) {
        Thread task = new SFlowAgent();
        task.setDaemon(true);
        task.setName("sFlow JVM Thread");
        task.setPriority(Thread.MIN_PRIORITY);
        task.start();
    }
}
