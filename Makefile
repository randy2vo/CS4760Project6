CXX = g++
CXXFLAGS = -Wall -g

all: oss worker

oss: oss.cpp shared.h
	$(CXX) $(CXXFLAGS) -o oss oss.cpp

worker: worker.cpp shared.h
	$(CXX) $(CXXFLAGS) -o worker worker.cpp

clean:
	rm -f oss worker *.o  *.swp *.log *.txt   || true
