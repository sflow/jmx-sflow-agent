The SFlowAgent class can be attached to existing applications using the java -javaagent command line argument. The SFlowAgent periodically exports JMX performance metrics to a central sFlow analyzer.

The java SFlowAgent is designed to work with the Host sFlow agent (which exports performance statistics from the host), sharing configuration settings. For more information, see:

http://blog.sflow.com/2011/09/java-virtual-machine.html