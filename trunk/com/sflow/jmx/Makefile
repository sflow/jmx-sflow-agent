JAVAC=javac
JARC=jar
OPT=-g 
 
JFLAGS= -source 1.5 -target 1.5
 
.SUFFIXES: .java .class
 
CLASSES= \
        SFlowAgent.class
 
all: build-classes build-jar
 
build-classes: $(CLASSES)

build-jar:
	cd ../../../; $(JARC) cvmf com/sflow/jmx/manifest.txt com/sflow/jmx/sflowagent.jar com/sflow/jmx/SFlowAgent.class

clean:
	rm -f *.class
	rm -f *.jar
 
#-----------------------------------------------------------
#generic rules
 
.java.class:
	$(JAVAC) $(JFLAGS) $(OPT) $*.java

