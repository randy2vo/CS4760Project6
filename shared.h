#ifndef SHARED_H
#define SHARED_H

#include <sys/types.h>

// Project 6 constants
const int TABLE_SIZE = 20;
const int MAX_ACTIVE_PROCS = 18;
const unsigned int BILLION = 1000000000U;
const unsigned int CLOCK_INCREMENT_NS = 10000000U; // 10 ms per oss loop step

const int PAGE_SIZE = 1024;          // 1 KB pages
const int PAGES_PER_PROCESS = 16;    // 16 KB logical address space per process
const int FRAME_COUNT = 64;          // 64 KB physical memory / 1 KB page size

// Message action values
const int ACTION_TERMINATE = 0;
const int ACTION_READ = 1;
const int ACTION_WRITE = 2;
const int ACTION_DISPATCH = 999;

struct SimClock {
    unsigned int seconds;
    unsigned int nanoseconds;
};

struct PCB {
    int occupied;
    pid_t pid;
    int localPid;

    unsigned int startSeconds;
    unsigned int startNano;

    unsigned int endSeconds;
    unsigned int endNano;

    int blocked;
    int pageTable[PAGES_PER_PROCESS];

    unsigned long memoryAccesses;
    unsigned long totalAccessTimeNs;
};

struct Message {
    long mtype;      // recipient message type; workers receive on getpid(), oss receives on 1
    int index;       // PCB slot index
    int action;      // 0 terminate, 1 read, 2 write, 999 dispatch/ack
    int address;     // logical memory address requested by worker
    int granted;     // ack/status field
};

#endif

