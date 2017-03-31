JAVA=java
JAVAC=javac
JAR=jar
LIB_CLASSPATH="lib/*"
COMPILE_CLASSPATH="src:${LIB_CLASSPATH}"
RUN_CLASSPATH="build/hw1.jar:${LIB_CLASSPATH}"

SOURCES=$(shell find . -type f -name '*.java')

MAPREDUCE_OPTS=-Dmapreduce.local.map.tasks.maximum=3 -Dmapreduce.local.reduce.tasks.maximum=3 -Dmapreduce.job.reduces=3

.PHONY: build clean wordcount tf idf tfidf tfsmall idfsmall clean

build: clean
	mkdir build
	mkdir build/classes
	$(JAVAC) -classpath $(COMPILE_CLASSPATH) -d build/classes $(SOURCES)
	cp log4j.properties build/classes
	$(JAR) cf build/hw1.jar -C build/classes .
	echo "Build Complete!"

wordcount: build
	rm -rf ./output-wordcount
	$(JAVA) -cp $(RUN_CLASSPATH) homework.WordCount $(MAPREDUCE_OPTS) ./data/wordcount ./output-wordcount

tf: build
	rm -rf ./output-tf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.TermFrequency $(MAPREDUCE_OPTS) ./data/tfidf ./output-tf

tfsmall: build
	rm -rf ./output-tf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.TermFrequency $(MAPREDUCE_OPTS) ./data/tfidf-small ./output-tf

idf: build
	rm -rf ./output-idf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.InverseDocumentFrequency $(MAPREDUCE_OPTS) ./data/tfidf ./output-idf 5006

idfsmall: build
	rm -rf ./output-idf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.InverseDocumentFrequency $(MAPREDUCE_OPTS) ./data/tfidf-small ./output-idf 3

tfidf: build
	rm -rf ./output-tfidf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.TFIDFJoin $(MAPREDUCE_OPTS) ./output-tf ./output-idf ./output-tfidf

clean:
	rm -rf build

