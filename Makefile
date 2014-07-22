# Makefile
# 15-640/440 Project 3 - Map Reduce 
# Abhishek Sharma (asharma3) and Douglas Rew (dkrew)

CODEBASEDIR = ./src
BINDIR = ./bin

default:
	find $(CODEBASEDIR) -name *.java > compile_list.txt
	javac -d $(BINDIR) @compile_list.txt

	rmic -classpath $(BINDIR) abhi.adfs.DataNodeImpl

	rmic -classpath $(BINDIR) abhi.adfs.NameNodeSlaveImpl

	rmic -classpath $(BINDIR) abhi.adfs.NameNodeMasterImpl

	rmic -classpath $(BINDIR) abhi.mapreduce.JobTrackerServiceProvider

	rmic -classpath $(BINDIR) abhi.mapreduce.TaskTrackerServices

	rm compile_list.txt

	echo Stub Generated and Code Compiled. We are Golden!

clean: 
		rm -r $(BINDIR)/*/*/*.class
