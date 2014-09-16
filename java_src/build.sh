export LIBS=libs/py4j0.8.2.1.jar:libs/hadoop-core-1.2.0.jar:libs/apache-nutch-1.10-SNAPSHOT.jar:libs/commons-cli-1.2.jar:libs/commons-logging-1.1.1.jar:libs/commons-configuration-1.6.jar:libs/commons-lang-2.6.jar

javac -classpath $LIBS SeqReaderApp.java
#jar -cvf seqreaderapp.jar
java -classpath .:$LIBS SeqReaderApp

