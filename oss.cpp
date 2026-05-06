#include <iostream>
#include <iomanip>
#include <string>
#include <queue>
#include <vector>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdarg>
#include <ctime>
#include <cmath>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include "shared.h"

using namespace std;

#ifndef TABLE_SIZE
#define TABLE_SIZE 20
#endif

#ifndef MAX_ACTIVE_PROCS
#define MAX_ACTIVE_PROCS 18
#endif

#ifndef BILLION
#define BILLION 1000000000U
#endif

// Clock incremented 1000 ns (1 µs) per loop tick so the 14 ms disk-I/O delay
// spans many iterations and the blocking simulation is meaningful.
#ifndef CLOCK_INCREMENT_NS
#define CLOCK_INCREMENT_NS 1000U
#endif

#ifndef PAGE_SIZE
#define PAGE_SIZE 1024
#endif

#ifndef PAGES_PER_PROCESS
#define PAGES_PER_PROCESS 16
#endif

#ifndef FRAME_COUNT
#define FRAME_COUNT 64
#endif

static const unsigned int MEMORY_ACCESS_NS = 100U;
static const unsigned int DISK_ACCESS_NS   = 14000000U; // 14 ms
static const unsigned int DIRTY_EXTRA_NS   = 14000000U; // extra write-back time
static const int LOG_LIMIT = 10000;

struct LocalPCB {
    int occupied = 0;
    pid_t pid = 0;
    int localPid = 0;
    unsigned int startSeconds = 0;
    unsigned int startNano = 0;
    unsigned int endSeconds = 0;
    unsigned int endNano = 0;
    int blocked = 0;
    int blockedAddress = -1;
    int blockedAction = 0;
    int pageTable[PAGES_PER_PROCESS];
    unsigned long memoryAccesses = 0;
    unsigned long totalAccessTimeNs = 0;
};

struct Frame {
    bool occupied = false;
    int dirty = 0;
    int processIndex = -1;
    int page = -1;
};

struct BlockedRequest {
    int processIndex = -1;
    int address = -1;
    int action = 0; // 1 read, 2 write
    unsigned int readySeconds = 0;
    unsigned int readyNano = 0;
};

static int g_shmid = -1;
static int g_msgid = -1;
static SimClock* g_clk = nullptr;
static LocalPCB g_table[TABLE_SIZE];
static Frame g_frames[FRAME_COUNT];
static FILE* g_logFile = nullptr;

static queue<int> g_fifoFrames;
static vector<BlockedRequest> g_blockedQueue;

static int g_logLines = 0;
static unsigned long g_totalRequests = 0;
static unsigned long g_totalReads = 0;
static unsigned long g_totalWrites = 0;
static unsigned long g_totalPageFaults = 0;
static unsigned long g_totalMemoryAccessTimeNs = 0;

static void logBoth(const char* fmt, ...) {
    va_list args1, args2;
    va_start(args1, fmt);
    va_copy(args2, args1);

    vprintf(fmt, args1);
    fflush(stdout);

    if (g_logFile && g_logLines < LOG_LIMIT) {
        vfprintf(g_logFile, fmt, args2);
        fflush(g_logFile);
        g_logLines++;
    }

    va_end(args2);
    va_end(args1);
}

static void normalizeTime(unsigned int& s, unsigned int& ns) {
    while (ns >= BILLION) {
        s++;
        ns -= BILLION;
    }
}

static void addToClock(unsigned int addNS) {
    if (!g_clk) return;
    g_clk->nanoseconds += addNS;
    normalizeTime(g_clk->seconds, g_clk->nanoseconds);
}

static bool timeGTE(unsigned int sA, unsigned int nA,
                    unsigned int sB, unsigned int nB) {
    return (sA > sB) || (sA == sB && nA >= nB);
}

static void addNsToCurrent(unsigned int addNS, unsigned int& outS, unsigned int& outNS) {
    outS = g_clk->seconds;
    outNS = g_clk->nanoseconds + addNS;
    normalizeTime(outS, outNS);
}

static unsigned int secondsPart(double x) {
    if (x <= 0.0) return 0;
    return (unsigned int)floor(x);
}

static unsigned int nanosPart(double x) {
    if (x <= 0.0) return 0;
    double whole = floor(x);
    double frac = x - whole;
    return (unsigned int)(frac * 1000000000.0);
}

static void clearPCB(int i) {
    g_table[i] = LocalPCB();
    for (int p = 0; p < PAGES_PER_PROCESS; p++) {
        g_table[i].pageTable[p] = -1;
    }
}

static void initTables() {
    for (int i = 0; i < TABLE_SIZE; i++) clearPCB(i);
    for (int f = 0; f < FRAME_COUNT; f++) g_frames[f] = Frame();
    while (!g_fifoFrames.empty()) g_fifoFrames.pop();
    g_blockedQueue.clear();
}

static void removeBlockedRequestForProcess(int idx) {
    vector<BlockedRequest> kept;
    for (const auto& req : g_blockedQueue) {
        if (req.processIndex != idx) kept.push_back(req);
    }
    g_blockedQueue.swap(kept);
}

static void rebuildFifoWithoutFramesOwnedBy(int idx) {
    queue<int> rebuilt;
    while (!g_fifoFrames.empty()) {
        int f = g_fifoFrames.front();
        g_fifoFrames.pop();
        if (f >= 0 && f < FRAME_COUNT && g_frames[f].occupied && g_frames[f].processIndex != idx) {
            rebuilt.push(f);
        }
    }
    g_fifoFrames.swap(rebuilt);
}

static void releaseProcessFrames(int idx) {
    for (int p = 0; p < PAGES_PER_PROCESS; p++) {
        int frame = g_table[idx].pageTable[p];
        if (frame >= 0 && frame < FRAME_COUNT) {
            g_frames[frame] = Frame();
            g_table[idx].pageTable[p] = -1;
        }
    }
    rebuildFifoWithoutFramesOwnedBy(idx);
    removeBlockedRequestForProcess(idx);
}

static void cleanup() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (g_table[i].occupied && g_table[i].pid > 0) {
            kill(g_table[i].pid, SIGTERM);
        }
    }

    while (waitpid(-1, nullptr, WNOHANG) > 0) {}

    if (g_clk && g_clk != (SimClock*)-1) {
        shmdt(g_clk);
        g_clk = nullptr;
    }

    if (g_shmid != -1) {
        shmctl(g_shmid, IPC_RMID, nullptr);
        g_shmid = -1;
    }

    if (g_msgid != -1) {
        msgctl(g_msgid, IPC_RMID, nullptr);
        g_msgid = -1;
    }

    if (g_logFile) {
        fclose(g_logFile);
        g_logFile = nullptr;
    }
}

static void signal_handler(int) {
    cleanup();
    _exit(1);
}

static void printHelp(const char* prog) {
    cout << "Usage: " << prog
         << " [-h] [-n proc] [-s simul] [-t timeLimitForChildren] "
         << "[-i fractionOfSecondToLaunchChildren] [-f logfile]\n";
}

static int findFreeSlot() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (!g_table[i].occupied) return i;
    }
    return -1;
}

static int pickRunnableProcess() {
    static int last = -1;
    for (int count = 0; count < TABLE_SIZE; count++) {
        int i = (last + 1 + count) % TABLE_SIZE;
        if (g_table[i].occupied && !g_table[i].blocked) {
            last = i;
            return i;
        }
    }
    return -1;
}

static int findFreeFrame() {
    for (int f = 0; f < FRAME_COUNT; f++) {
        if (!g_frames[f].occupied) return f;
    }
    return -1;
}

static int chooseVictimFrameFIFO() {
    while (!g_fifoFrames.empty()) {
        int f = g_fifoFrames.front();
        g_fifoFrames.pop();
        if (f >= 0 && f < FRAME_COUNT && g_frames[f].occupied) {
            return f;
        }
    }
    // FIFO queue exhausted with no occupied frame found — return -1 so the
    // caller can detect and handle the error instead of silently evicting frame 0.
    return -1;
}

static void mapPageIntoFrame(int idx, int page, int frame, bool isWrite) {
    int oldProc = g_frames[frame].processIndex;
    int oldPage = g_frames[frame].page;

    if (oldProc >= 0 && oldProc < TABLE_SIZE && oldPage >= 0 && oldPage < PAGES_PER_PROCESS) {
        g_table[oldProc].pageTable[oldPage] = -1;
    }

    g_frames[frame].occupied = true;
    g_frames[frame].dirty = isWrite ? 1 : 0;
    g_frames[frame].processIndex = idx;
    g_frames[frame].page = page;
    g_table[idx].pageTable[page] = frame;
    g_fifoFrames.push(frame);
}

static void sendAckToProcess(int idx) {
    Message ack;
    memset(&ack, 0, sizeof(ack));
    ack.mtype = g_table[idx].pid;
    ack.index = idx;
    ack.action = 999;
    ack.granted = 1;

    if (msgsnd(g_msgid, &ack, sizeof(Message) - sizeof(long), 0) == -1) {
        logBoth("OSS: failed sending ACK to P%d: %s\n", g_table[idx].localPid, strerror(errno));
    }
}

static void printBlockedList() {
    logBoth("Blocked queue [");
    for (const auto& req : g_blockedQueue) {
        if (req.processIndex >= 0 && req.processIndex < TABLE_SIZE && g_table[req.processIndex].occupied) {
            logBoth(" P%d(addr=%d %s ready=%u:%u)",
                    g_table[req.processIndex].localPid,
                    req.address,
                    req.action == 2 ? "W" : "R",
                    req.readySeconds,
                    req.readyNano);
        }
    }
    logBoth(" ]\n");
}

static void printFrameTable() {
    logBoth("Frame Table:\n");
    logBoth("Frame Occupied DirtyBit Process Page\n");
    for (int f = 0; f < FRAME_COUNT; f++) {
        logBoth("Frame %-2d: %-3s %-8d %-7d %-4d\n",
                f,
                g_frames[f].occupied ? "Yes" : "No",
                g_frames[f].dirty,
                g_frames[f].processIndex >= 0 ? g_table[g_frames[f].processIndex].localPid : -1,
                g_frames[f].page);
    }
}

static void printPageTables() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (!g_table[i].occupied) continue;
        logBoth("P%d page table: [ ", g_table[i].localPid);
        for (int p = 0; p < PAGES_PER_PROCESS; p++) {
            logBoth("%d ", g_table[i].pageTable[p]);
        }
        logBoth("]\n");
    }
}

static void printProcessTable() {
    logBoth("\nOSS PID:%d SysClockS:%u SysClockNano:%u\n",
            getpid(), g_clk->seconds, g_clk->nanoseconds);
    logBoth("Process Table:\n");
    logBoth("Entry Occupied PID LocalPID StartS StartN EndS EndN Blocked Accesses AvgAccess(ns)\n");

    for (int i = 0; i < TABLE_SIZE; i++) {
        LocalPCB& p = g_table[i];
        unsigned long avg = p.memoryAccesses == 0 ? 0 : p.totalAccessTimeNs / p.memoryAccesses;
        logBoth("%-5d %-8d %-8d %-8d %-6u %-8u %-4u %-8u %-7d %-8lu %-12lu\n",
                i, p.occupied ? 1 : 0, (int)p.pid, p.localPid,
                p.startSeconds, p.startNano, p.endSeconds, p.endNano,
                p.blocked ? 1 : 0, p.memoryAccesses, avg);
    }

    printBlockedList();

    // Memory layout header appears just before the frame/page table output
    // to match the spec's example format.
    logBoth("\nCurrent memory layout at time %u:%u is:\n",
            g_clk->seconds, g_clk->nanoseconds);
    printFrameTable();
    printPageTables();
    logBoth("\n");
}

static bool allActiveProcessesBlocked() {
    bool sawActive = false;
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (g_table[i].occupied) {
            sawActive = true;
            if (!g_table[i].blocked) return false;
        }
    }
    return sawActive && !g_blockedQueue.empty();
}

static void serviceReadyBlockedRequests() {
    bool changed = true;
    while (changed) {
        changed = false;
        for (auto it = g_blockedQueue.begin(); it != g_blockedQueue.end(); ) {
            int idx = it->processIndex;
            if (idx < 0 || idx >= TABLE_SIZE || !g_table[idx].occupied) {
                it = g_blockedQueue.erase(it);
                changed = true;
                continue;
            }

            if (timeGTE(g_clk->seconds, g_clk->nanoseconds, it->readySeconds, it->readyNano)) {
                int page = it->address / PAGE_SIZE;
                bool isWrite = (it->action == 2);
                int frame = findFreeFrame();
                bool dirtyVictim = false;

                if (frame == -1) {
                    frame = chooseVictimFrameFIFO();
                    // Guard: if FIFO returned -1, bookkeeping is broken — skip
                    // this request rather than silently corrupting frame 0.
                    if (frame == -1) {
                        logBoth("oss: ERROR no victim frame available for P%d page %d; deferring\n",
                                g_table[idx].localPid, page);
                        ++it;
                        continue;
                    }
                    dirtyVictim = g_frames[frame].dirty != 0;
                    logBoth("oss: Clearing frame %d and swapping in P%d page %d\n",
                            frame, g_table[idx].localPid, page);
                    if (dirtyVictim) {
                        logBoth("oss: Dirty bit of frame %d set, adding additional time to the clock\n", frame);
                        addToClock(DIRTY_EXTRA_NS);
                        g_totalMemoryAccessTimeNs += DIRTY_EXTRA_NS;
                        g_table[idx].totalAccessTimeNs += DIRTY_EXTRA_NS;
                    }
                } else {
                    logBoth("oss: Using free frame %d and swapping in P%d page %d\n",
                            frame, g_table[idx].localPid, page);
                }

                mapPageIntoFrame(idx, page, frame, isWrite);
                g_table[idx].blocked = 0;
                g_table[idx].blockedAddress = -1;
                g_table[idx].blockedAction = 0;

                logBoth("oss: Indicating to P%d that %s has happened to address %05d at time %u:%u\n",
                        g_table[idx].localPid,
                        isWrite ? "write" : "read",
                        it->address,
                        g_clk->seconds,
                        g_clk->nanoseconds);

                sendAckToProcess(idx);
                it = g_blockedQueue.erase(it);
                changed = true;
            } else {
                ++it;
            }
        }
    }
}

static void advanceClockIfSoftBlocked() {
    if (!allActiveProcessesBlocked()) return;

    unsigned int nextS = g_blockedQueue[0].readySeconds;
    unsigned int nextNS = g_blockedQueue[0].readyNano;
    for (const auto& req : g_blockedQueue) {
        if (!timeGTE(req.readySeconds, req.readyNano, nextS, nextNS)) {
            nextS = req.readySeconds;
            nextNS = req.readyNano;
        }
    }

    if (!timeGTE(g_clk->seconds, g_clk->nanoseconds, nextS, nextNS)) {
        logBoth("oss: All active processes blocked; advancing clock to %u:%u to prevent soft deadlock\n",
                nextS, nextNS);
        g_clk->seconds = nextS;
        g_clk->nanoseconds = nextNS;
    }
}

static void blockForPageFault(int idx, int address, int action) {
    bool isWrite = (action == 2);
    int page = address / PAGE_SIZE;

    g_totalPageFaults++;
    g_table[idx].blocked = 1;
    g_table[idx].blockedAddress = address;
    g_table[idx].blockedAction = action;

    BlockedRequest req;
    req.processIndex = idx;
    req.address = address;
    req.action = action;
    addNsToCurrent(DISK_ACCESS_NS, req.readySeconds, req.readyNano);

    g_blockedQueue.push_back(req);
    g_totalMemoryAccessTimeNs += DISK_ACCESS_NS;
    g_table[idx].totalAccessTimeNs += DISK_ACCESS_NS;

    logBoth("oss: Address %05d from P%d page %d is not in a frame, pagefault at time %u:%u\n",
            address, g_table[idx].localPid, page, g_clk->seconds, g_clk->nanoseconds);
    logBoth("oss: Queuing disk %s for P%d until time %u:%u\n",
            isWrite ? "write" : "read",
            g_table[idx].localPid,
            req.readySeconds,
            req.readyNano);
}

static void handleMemoryRequest(const Message& reply) {
    int idx = reply.index;
    if (idx < 0 || idx >= TABLE_SIZE || !g_table[idx].occupied) return;

    int action = reply.action;
    int address = reply.address;
    bool isWrite = (action == 2);
    bool isRead = (action == 1);

    if (!isRead && !isWrite) return;

    if (address < 0 || address >= PAGES_PER_PROCESS * PAGE_SIZE) {
        logBoth("oss: P%d made invalid address request %d; ignoring request\n",
                g_table[idx].localPid, address);
        sendAckToProcess(idx);
        return;
    }

    int page = address / PAGE_SIZE;
    g_totalRequests++;
    g_table[idx].memoryAccesses++;
    if (isWrite) g_totalWrites++; else g_totalReads++;

    logBoth("oss: P%d requesting %s of address %05d at time %u:%u\n",
            g_table[idx].localPid,
            isWrite ? "write" : "read",
            address,
            g_clk->seconds,
            g_clk->nanoseconds);

    int frame = g_table[idx].pageTable[page];
    if (frame >= 0 && frame < FRAME_COUNT && g_frames[frame].occupied) {
        if (isWrite) g_frames[frame].dirty = 1;
        addToClock(MEMORY_ACCESS_NS);
        g_totalMemoryAccessTimeNs += MEMORY_ACCESS_NS;
        g_table[idx].totalAccessTimeNs += MEMORY_ACCESS_NS;

        logBoth("oss: Address %05d in frame %d, %s data for P%d at time %u:%u\n",
                address,
                frame,
                isWrite ? "writing" : "giving",
                g_table[idx].localPid,
                g_clk->seconds,
                g_clk->nanoseconds);

        sendAckToProcess(idx);
    } else {
        blockForPageFault(idx, address, action);
    }
}

static void terminateProcess(int idx, int& activeChildren) {
    if (idx < 0 || idx >= TABLE_SIZE || !g_table[idx].occupied) return;

    unsigned long avg = g_table[idx].memoryAccesses == 0
        ? 0
        : g_table[idx].totalAccessTimeNs / g_table[idx].memoryAccesses;

    logBoth("oss: Process P%d terminating at time %u:%u. Effective memory access time: %lu ns\n",
            g_table[idx].localPid, g_clk->seconds, g_clk->nanoseconds, avg);

    releaseProcessFrames(idx);
    // Use WNOHANG to avoid blocking if the worker hasn't exited yet;
    // reapExitedChildren() will catch it on a future iteration.
    waitpid(g_table[idx].pid, nullptr, WNOHANG);
    clearPCB(idx);
    activeChildren--;
}

static void reapExitedChildren(int& activeChildren) {
    while (true) {
        int status = 0;
        pid_t dead = waitpid(-1, &status, WNOHANG);
        if (dead <= 0) break;

        for (int i = 0; i < TABLE_SIZE; i++) {
            if (g_table[i].occupied && g_table[i].pid == dead) {
                logBoth("oss: Reaped child P%d that exited unexpectedly at time %u:%u\n",
                        g_table[i].localPid, g_clk->seconds, g_clk->nanoseconds);
                releaseProcessFrames(i);
                clearPCB(i);
                activeChildren--;
                break;
            }
        }
    }
}

static void reapExpiredBlockedProcesses(int& activeChildren) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (g_table[i].occupied && g_table[i].blocked &&
            timeGTE(g_clk->seconds, g_clk->nanoseconds,
                    g_table[i].endSeconds, g_table[i].endNano)) {
            logBoth("oss: Terminating blocked P%d because its time expired at %u:%u\n",
                    g_table[i].localPid, g_clk->seconds, g_clk->nanoseconds);
            kill(g_table[i].pid, SIGTERM);
            waitpid(g_table[i].pid, nullptr, 0);
            releaseProcessFrames(i);
            clearPCB(i);
            activeChildren--;
        }
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGALRM, signal_handler);
    alarm(5);  // Hard real-time kill after 5 seconds per spec; prevents hangs.

    srand((unsigned int)(time(nullptr) ^ getpid()));

    int n = 1;
    int s = 1;
    double t = 2.0;
    double interval = 0.1;
    string logFilename = "log.txt";

    int opt;
    while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
        switch (opt) {
            case 'h':
                printHelp(argv[0]);
                return 0;
            case 'n':
                n = atoi(optarg);
                if (n <= 0) {
                    cerr << "Error: -n must be > 0\n";
                    return 1;
                }
                break;
            case 's':
                s = atoi(optarg);
                if (s <= 0) {
                    cerr << "Error: -s must be > 0\n";
                    return 1;
                }
                break;
            case 't':
                t = atof(optarg);
                if (t <= 0.0) {
                    cerr << "Error: -t must be > 0\n";
                    return 1;
                }
                break;
            case 'i':
                interval = atof(optarg);
                if (interval < 0.0) {
                    cerr << "Error: -i must be >= 0\n";
                    return 1;
                }
                break;
            case 'f':
                logFilename = optarg;
                break;
            default:
                printHelp(argv[0]);
                return 1;
        }
    }

    if (s > n) s = n;
    if (s > MAX_ACTIVE_PROCS) s = MAX_ACTIVE_PROCS;

    g_logFile = fopen(logFilename.c_str(), "w");
    if (!g_logFile) {
        cerr << "OSS: failed to open log file: " << logFilename << "\n";
        return 1;
    }

    key_t shmKey = ftok(".", 'C');
    if (shmKey == -1) {
        cerr << "OSS: ftok shared memory failed: " << strerror(errno) << "\n";
        cleanup();
        return 1;
    }

    g_shmid = shmget(shmKey, sizeof(SimClock), 0666 | IPC_CREAT);
    if (g_shmid == -1) {
        cerr << "OSS: shmget failed: " << strerror(errno) << "\n";
        cleanup();
        return 1;
    }

    g_clk = (SimClock*)shmat(g_shmid, nullptr, 0);
    if (g_clk == (SimClock*)-1) {
        cerr << "OSS: shmat failed: " << strerror(errno) << "\n";
        cleanup();
        return 1;
    }

    g_clk->seconds = 0;
    g_clk->nanoseconds = 0;

    key_t msgKey = ftok(".", 'Q');
    if (msgKey == -1) {
        cerr << "OSS: ftok message queue failed: " << strerror(errno) << "\n";
        cleanup();
        return 1;
    }

    g_msgid = msgget(msgKey, 0666 | IPC_CREAT);
    if (g_msgid == -1) {
        cerr << "OSS: msgget failed: " << strerror(errno) << "\n";
        cleanup();
        return 1;
    }

    initTables();

    int launched = 0;
    int activeChildren = 0;
    int nextLocalPid = 1;

    unsigned int nextLaunchS = 0;
    unsigned int nextLaunchNS = 0;
    unsigned int nextPrintS = 0;
    unsigned int nextPrintNS = 500000000U;

    unsigned int intervalSec = secondsPart(interval);
    unsigned int intervalNano = nanosPart(interval);

    time_t realStart = time(nullptr);

    while ((launched < n && difftime(time(nullptr), realStart) < 5.0) || activeChildren > 0) {
        reapExitedChildren(activeChildren);
        serviceReadyBlockedRequests();
        advanceClockIfSoftBlocked();
        serviceReadyBlockedRequests();

        if (launched < n &&
            difftime(time(nullptr), realStart) < 5.0 &&
            activeChildren < s &&
            activeChildren < MAX_ACTIVE_PROCS &&
            timeGTE(g_clk->seconds, g_clk->nanoseconds, nextLaunchS, nextLaunchNS)) {

            int slot = findFreeSlot();
            if (slot != -1) {
                unsigned int endS = g_clk->seconds + (unsigned int)t;
                unsigned int endNS = g_clk->nanoseconds + nanosPart(t);
                normalizeTime(endS, endNS);

                pid_t child = fork();
                if (child == 0) {
                    string idxStr = to_string(slot);
                    string secStr = to_string(endS);
                    string nanoStr = to_string(endNS);

                    execl("./worker", "worker",
                          idxStr.c_str(),
                          secStr.c_str(),
                          nanoStr.c_str(),
                          (char*)nullptr);

                    cerr << "OSS: execl failed: " << strerror(errno) << "\n";
                    _exit(1);
                } else if (child > 0) {
                    clearPCB(slot);
                    g_table[slot].occupied = 1;
                    g_table[slot].pid = child;
                    g_table[slot].localPid = nextLocalPid++;
                    g_table[slot].startSeconds = g_clk->seconds;
                    g_table[slot].startNano = g_clk->nanoseconds;
                    g_table[slot].endSeconds = endS;
                    g_table[slot].endNano = endNS;
                    g_table[slot].blocked = 0;

                    launched++;
                    activeChildren++;

                    unsigned int tempS = g_clk->seconds + intervalSec;
                    unsigned int tempNS = g_clk->nanoseconds + intervalNano;
                    normalizeTime(tempS, tempNS);
                    nextLaunchS = tempS;
                    nextLaunchNS = tempNS;

                    logBoth("OSS: Generating process with local PID %d in slot %d at time %u:%u\n",
                            g_table[slot].localPid, slot,
                            g_clk->seconds, g_clk->nanoseconds);
                } else {
                    cerr << "OSS: fork failed: " << strerror(errno) << "\n";
                }
            }
        }

        addToClock(CLOCK_INCREMENT_NS);
        reapExpiredBlockedProcesses(activeChildren);

        int picked = pickRunnableProcess();
        if (picked != -1) {
            Message msg;
            memset(&msg, 0, sizeof(msg));
            msg.mtype = g_table[picked].pid;
            msg.index = picked;
            msg.action = 999;
            msg.granted = 1;

            if (msgsnd(g_msgid, &msg, sizeof(Message) - sizeof(long), 0) == -1) {
                cerr << "OSS: msgsnd dispatch failed: " << strerror(errno) << "\n";
                cleanup();
                return 1;
            }

            Message reply;
            memset(&reply, 0, sizeof(reply));
            if (msgrcv(g_msgid, &reply, sizeof(Message) - sizeof(long), 1, 0) == -1) {
                if (errno == EINTR) continue;
                cerr << "OSS: msgrcv failed: " << strerror(errno) << "\n";
                cleanup();
                return 1;
            }

            if (reply.action == 0) {
                terminateProcess(reply.index, activeChildren);
            } else {
                handleMemoryRequest(reply);
            }
        }

        addToClock(CLOCK_INCREMENT_NS);

        if (timeGTE(g_clk->seconds, g_clk->nanoseconds, nextPrintS, nextPrintNS)) {
            printProcessTable();
            nextPrintS = g_clk->seconds;
            nextPrintNS = g_clk->nanoseconds + 500000000U;
            normalizeTime(nextPrintS, nextPrintNS);
        }
    }

    logBoth("\nOSS Summary:\n");
    logBoth("Total memory requests: %lu\n", g_totalRequests);
    logBoth("Total reads: %lu\n", g_totalReads);
    logBoth("Total writes: %lu\n", g_totalWrites);
    logBoth("Total page faults: %lu\n", g_totalPageFaults);

    double faultPct = 0.0;
    if (g_totalRequests > 0) {
        faultPct = ((double)g_totalPageFaults / (double)g_totalRequests) * 100.0;
    }
    logBoth("Page fault percentage: %.2f%%\n", faultPct);

    unsigned long avgAccess = g_totalRequests == 0 ? 0 : g_totalMemoryAccessTimeNs / g_totalRequests;
    logBoth("Average effective memory access time: %lu ns\n", avgAccess);

    cleanup();
    return 0;
}
