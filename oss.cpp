/*
 * Author: Randy Vo
 * Date: May 2026
 * Course: CS 4760 - Operating Systems
 * Assignment 6 - Memory Management / FIFO Page Replacement
 *
 * oss.cpp
 * Main OS simulator. Manages a 64-frame physical memory space shared among
 * up to 18 worker processes. Each worker has a 16-page (16 KB) logical address
 * space with a 1 KB page size. When a worker requests a memory address, oss
 * checks if the page is already in a frame (page hit) or must be loaded from
 * disk (page fault). On a fault, the process is blocked for a simulated 14 ms
 * disk I/O delay. If all frames are full, the oldest loaded frame is evicted
 * using FIFO. If the evicted frame was written to (dirty bit set), an extra
 * 14 ms write-back penalty is added to the clock. oss also handles a "soft
 * deadlock" condition where all processes are simultaneously waiting for disk
 * I/O by advancing the simulated clock to the nearest unblock time.
 *
 * IPC used:
 *   - One shared memory segment for the simulated clock (SimClock)
 *   - One System V message queue for all oss <-> worker communication
 *
 * Message protocol:
 *   oss -> worker:  mtype = worker's real PID, action = 999 (dispatch/ack)
 *   worker -> oss:  mtype = 1, action = 0 (terminate) | 1 (read) | 2 (write)
 */

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

// ─── Fallback constants (shared.h should define these) ───────────────────────
#ifndef TABLE_SIZE
#define TABLE_SIZE 20          // max number of PCB slots
#endif

#ifndef MAX_ACTIVE_PROCS
#define MAX_ACTIVE_PROCS 18    // max simultaneously running workers
#endif

#ifndef BILLION
#define BILLION 1000000000U    // nanoseconds per second
#endif

// Each main loop tick advances the clock by 1 µs (1000 ns).
// Keeping this small means the 14 ms disk-I/O delay spans ~14,000 loop
// iterations, so blocked processes stay blocked for a realistic amount of
// simulated time rather than being immediately unblocked.
#ifndef CLOCK_INCREMENT_NS
#define CLOCK_INCREMENT_NS 1000U
#endif

#ifndef PAGE_SIZE
#define PAGE_SIZE 1024         // bytes per page (1 KB)
#endif

#ifndef PAGES_PER_PROCESS
#define PAGES_PER_PROCESS 16   // each process has 16 pages = 16 KB logical space
#endif

#ifndef FRAME_COUNT
#define FRAME_COUNT 64         // 64 physical frames = 64 KB total memory
#endif

// Time cost constants
static const unsigned int MEMORY_ACCESS_NS = 100U;       // page hit costs 100 ns
static const unsigned int DISK_ACCESS_NS   = 14000000U;  // page fault disk I/O: 14 ms
static const unsigned int DIRTY_EXTRA_NS   = 14000000U;  // extra 14 ms if evicted frame is dirty

// Maximum lines written to the log file before we stop logging
// (keeps the log from growing unbounded on long runs)
static const int LOG_LIMIT = 10000;

// ─── Data structures ──────────────────────────────────────────────────────────

/*
 * LocalPCB - Process Control Block
 * One entry per worker process slot. Tracks the worker's PID, its simulated
 * time limit, whether it is currently blocked waiting for disk I/O, and
 * per-process memory access statistics. Also embeds that process's page table
 * (pageTable[p] = frame index, or -1 if page p is not currently in memory).
 */
struct LocalPCB {
    int occupied = 0;                      // 1 if this slot holds a live process
    pid_t pid = 0;                         // real OS process ID
    int localPid = 0;                      // display-friendly sequential number (P1, P2, ...)
    unsigned int startSeconds = 0;         // sim clock when the process was launched
    unsigned int startNano = 0;
    unsigned int endSeconds = 0;           // sim clock deadline — process terminates after this
    unsigned int endNano = 0;
    int blocked = 0;                       // 1 = waiting on disk I/O (page fault in progress)
    int blockedAddress = -1;               // address that triggered the page fault
    int blockedAction = 0;                 // original action (1=read, 2=write) that caused fault
    int pageTable[PAGES_PER_PROCESS];      // pageTable[p] = physical frame holding logical page p
                                           //               -1 means the page is not in memory
    unsigned long memoryAccesses = 0;      // total number of memory requests made by this process
    unsigned long totalAccessTimeNs = 0;   // cumulative simulated access time (ns)
};

/*
 * Frame - one entry in the physical frame table
 * Tracks which process/page is currently occupying each physical frame,
 * and whether the frame has been written to since it was loaded (dirty bit).
 */
struct Frame {
    bool occupied = false;   // true if a page is currently loaded here
    int dirty = 0;           // 1 if this frame has been written to since it was loaded;
                             // on eviction, a dirty frame costs an extra 14 ms write-back
    int processIndex = -1;   // PCB table index of the owning process (-1 if free)
    int page = -1;           // logical page number stored here (-1 if free)
};

/*
 * BlockedRequest - one entry in the disk I/O wait queue
 * When a page fault occurs, the faulting process is blocked and a
 * BlockedRequest is pushed onto g_blockedQueue. oss services these in
 * order (one disk request at a time) using the ready time to decide when
 * each request is complete.
 */
struct BlockedRequest {
    int processIndex = -1;       // PCB slot of the blocked process
    int address = -1;            // logical address that caused the fault
    int action = 0;              // 1 = read fault, 2 = write fault
    unsigned int readySeconds = 0;  // sim clock time when this disk I/O will be done
    unsigned int readyNano = 0;
};

// ─── Global state ─────────────────────────────────────────────────────────────
static int       g_shmid = -1;          // shared memory ID for SimClock
static int       g_msgid = -1;          // message queue ID
static SimClock* g_clk   = nullptr;     // pointer into shared memory clock

static LocalPCB g_table[TABLE_SIZE];    // PCB table: one slot per possible worker
static Frame    g_frames[FRAME_COUNT];  // physical frame table: 64 frames
static FILE*    g_logFile = nullptr;    // log file handle (output goes here AND stdout)

// FIFO eviction queue: holds frame indices in the order they were loaded.
// The front of the queue is always the oldest loaded frame (evicted first).
static queue<int> g_fifoFrames;

// Disk I/O wait queue: blocked page-fault requests in arrival order.
// Only the head is "in service" at any given time.
static vector<BlockedRequest> g_blockedQueue;

// ─── Statistics counters ──────────────────────────────────────────────────────
static int g_logLines = 0;                        // lines written to log so far
static unsigned long g_totalRequests  = 0;        // total memory requests (reads + writes)
static unsigned long g_totalReads     = 0;        // read requests
static unsigned long g_totalWrites    = 0;        // write requests
static unsigned long g_totalPageFaults = 0;       // requests that caused a page fault
static unsigned long g_totalMemoryAccessTimeNs = 0; // cumulative access time across all processes

// ─── logBoth ─────────────────────────────────────────────────────────────────
/*
 * Writes a formatted message to both stdout and the log file simultaneously.
 * Stops writing to the log file once LOG_LIMIT lines have been written to
 * keep the file from growing too large, but always continues printing to
 * stdout so the grader can see what is happening in real time.
 */
static void logBoth(const char* fmt, ...) {
    va_list args1, args2;
    va_start(args1, fmt);
    va_copy(args2, args1);         // need two copies: one for printf, one for fprintf

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

// ─── normalizeTime ────────────────────────────────────────────────────────────
/*
 * Carries nanosecond overflow into the seconds field.
 * Called whenever nanoseconds might have exceeded 1,000,000,000 after
 * an addition — e.g. after addToClock or after computing a future timestamp.
 * Example: seconds=1, nanoseconds=1,500,000,000  ->  seconds=2, nano=500,000,000
 */
static void normalizeTime(unsigned int& s, unsigned int& ns) {
    while (ns >= BILLION) {
        s++;
        ns -= BILLION;
    }
}

// ─── addToClock ───────────────────────────────────────────────────────────────
/*
 * Advances the simulated clock by addNS nanoseconds.
 * Normalizes the result so nanoseconds never exceeds 999,999,999.
 * All simulated "time passing" in the system goes through this function.
 */
static void addToClock(unsigned int addNS) {
    if (!g_clk) return;
    g_clk->nanoseconds += addNS;
    normalizeTime(g_clk->seconds, g_clk->nanoseconds);
}

// ─── timeGTE ──────────────────────────────────────────────────────────────────
/*
 * Returns true if time point A (sA:nA) is >= time point B (sB:nB).
 * Used throughout to compare sim-clock timestamps — e.g. to check whether
 * a blocked process's disk I/O ready time has been reached, or whether it's
 * time to launch the next child or print the periodic report.
 */
static bool timeGTE(unsigned int sA, unsigned int nA,
                    unsigned int sB, unsigned int nB) {
    return (sA > sB) || (sA == sB && nA >= nB);
}

// ─── secondsPart / nanosPart ──────────────────────────────────────────────────
/*
 * Helpers that split a fractional-second value (like the -t or -i CLI args)
 * into separate whole-seconds and nanoseconds components for use in sim-clock
 * arithmetic.  Example: 2.5  ->  secondsPart = 2,  nanosPart = 500,000,000
 */
static unsigned int secondsPart(double x) {
    if (x <= 0.0) return 0;
    return (unsigned int)floor(x);
}
static unsigned int nanosPart(double x) {
    if (x <= 0.0) return 0;
    double frac = x - floor(x);
    return (unsigned int)(frac * 1000000000.0);
}

// ─── clearPCB ─────────────────────────────────────────────────────────────────
/*
 * Resets a PCB slot back to its default "empty" state.
 * Called when a process terminates or is killed so the slot can be reused
 * by the next worker that gets launched. All page table entries are set to
 * -1 to indicate "not in memory".
 */
static void clearPCB(int i) {
    g_table[i] = LocalPCB();                       // reset all fields to defaults
    for (int p = 0; p < PAGES_PER_PROCESS; p++) {
        g_table[i].pageTable[p] = -1;              // no pages loaded yet
    }
}

// ─── initTables ───────────────────────────────────────────────────────────────
/*
 * One-time startup initialization called from main() before any children
 * are forked. Clears all PCB slots, resets all frames to unoccupied, and
 * empties the FIFO eviction queue and the disk I/O blocked queue.
 */
static void initTables() {
    for (int i = 0; i < TABLE_SIZE; i++) clearPCB(i);
    for (int f = 0; f < FRAME_COUNT; f++) g_frames[f] = Frame();
    while (!g_fifoFrames.empty()) g_fifoFrames.pop();
    g_blockedQueue.clear();
}

// ─── removeBlockedRequestForProcess ──────────────────────────────────────────
/*
 * Removes all disk I/O wait-queue entries that belong to process `idx`.
 * Called when a process terminates (normally or forcibly) so its pending
 * page-fault requests don't linger in the queue and get serviced for a
 * process that no longer exists.
 */
static void removeBlockedRequestForProcess(int idx) {
    vector<BlockedRequest> kept;
    for (const auto& req : g_blockedQueue) {
        if (req.processIndex != idx) kept.push_back(req);
    }
    g_blockedQueue.swap(kept);
}

// ─── rebuildFifoWithoutFramesOwnedBy ─────────────────────────────────────────
/*
 * Rebuilds the FIFO eviction queue, skipping any entries that belong to
 * process `idx`. Called when process `idx` terminates so its frames are
 * removed from eviction consideration (they will be freed, not evicted).
 * Without this, a freed frame's index might remain in the FIFO queue and
 * later be "evicted" again, causing corrupt state.
 */
static void rebuildFifoWithoutFramesOwnedBy(int idx) {
    queue<int> rebuilt;
    while (!g_fifoFrames.empty()) {
        int f = g_fifoFrames.front();
        g_fifoFrames.pop();
        // Keep this frame in the queue only if it is still occupied and
        // belongs to a different process
        if (f >= 0 && f < FRAME_COUNT &&
            g_frames[f].occupied &&
            g_frames[f].processIndex != idx) {
            rebuilt.push(f);
        }
    }
    g_fifoFrames.swap(rebuilt);
}

// ─── releaseProcessFrames ─────────────────────────────────────────────────────
/*
 * Frees all physical frames that were held by process `idx` when it exits.
 * Steps:
 *   1. Walk the process's page table; for each valid entry, clear that frame.
 *   2. Remove stale entries from the FIFO eviction queue.
 *   3. Remove any pending disk I/O requests for this process from the queue.
 * After this call, every frame the process used is available for reuse.
 */
static void releaseProcessFrames(int idx) {
    // Step 1: free each physical frame this process owns
    for (int p = 0; p < PAGES_PER_PROCESS; p++) {
        int frame = g_table[idx].pageTable[p];
        if (frame >= 0 && frame < FRAME_COUNT) {
            g_frames[frame] = Frame();         // reset frame to unoccupied
            g_table[idx].pageTable[p] = -1;   // clear page table entry
        }
    }
    // Step 2: prune FIFO queue of any frames that belonged to this process
    rebuildFifoWithoutFramesOwnedBy(idx);
    // Step 3: remove pending I/O requests for this process
    removeBlockedRequestForProcess(idx);
}

// ─── cleanup ─────────────────────────────────────────────────────────────────
/*
 * Tears down all IPC resources and kills any remaining child processes.
 * Called on normal exit and from the signal handler on SIGINT/SIGALRM.
 * Order matters: kill children first, then detach/remove shared memory,
 * then remove the message queue, then close the log file.
 * After this function returns, no shared memory segments or message queues
 * belonging to this program should remain in the system.
 */
static void cleanup() {
    // Kill all live worker processes
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (g_table[i].occupied && g_table[i].pid > 0) {
            kill(g_table[i].pid, SIGTERM);
        }
    }
    // Reap any zombies so they don't linger
    while (waitpid(-1, nullptr, WNOHANG) > 0) {}

    // Detach and remove shared memory
    if (g_clk && g_clk != (SimClock*)-1) {
        shmdt(g_clk);
        g_clk = nullptr;
    }
    if (g_shmid != -1) {
        shmctl(g_shmid, IPC_RMID, nullptr);
        g_shmid = -1;
    }

    // Remove the message queue
    if (g_msgid != -1) {
        msgctl(g_msgid, IPC_RMID, nullptr);
        g_msgid = -1;
    }

    // Close the log file
    if (g_logFile) {
        fclose(g_logFile);
        g_logFile = nullptr;
    }
}

// ─── signal_handler ───────────────────────────────────────────────────────────
/*
 * Handles SIGINT (Ctrl-C) and SIGALRM (5-second real-time watchdog).
 * Calls cleanup() to free all IPC resources before exiting, so no shared
 * memory or message queues are left behind after an interrupt.
 */
static void signal_handler(int) {
    cleanup();
    _exit(1);
}

// ─── printHelp ────────────────────────────────────────────────────────────────
/* Prints command-line usage information and exits. */
static void printHelp(const char* prog) {
    cout << "Usage: " << prog
         << " [-h] [-n proc] [-s simul] [-t timeLimitForChildren] "
         << "[-i fractionOfSecondToLaunchChildren] [-f logfile]\n";
}

// ─── findFreeSlot ─────────────────────────────────────────────────────────────
/*
 * Scans the PCB table for the first unoccupied slot and returns its index.
 * Returns -1 if all slots are currently in use (shouldn't happen if s <= TABLE_SIZE).
 */
static int findFreeSlot() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (!g_table[i].occupied) return i;
    }
    return -1;
}

// ─── pickRunnableProcess ──────────────────────────────────────────────────────
/*
 * Round-robin scheduler: finds the next occupied, non-blocked process after
 * the last one that was dispatched. Uses a static cursor `last` to remember
 * where it left off so the same process is not repeatedly chosen.
 * Returns the PCB index of the chosen process, or -1 if no runnable process
 * exists (e.g. all processes are blocked waiting for disk I/O).
 */
static int pickRunnableProcess() {
    static int last = -1;   // index of the last process that was dispatched
    for (int count = 0; count < TABLE_SIZE; count++) {
        int i = (last + 1 + count) % TABLE_SIZE;
        if (g_table[i].occupied && !g_table[i].blocked) {
            last = i;
            return i;
        }
    }
    return -1;
}

// ─── findFreeFrame ────────────────────────────────────────────────────────────
/*
 * Scans the frame table for the first unoccupied physical frame.
 * Returns its index, or -1 if all 64 frames are currently in use (full memory).
 * When -1 is returned, the caller must evict an existing frame via FIFO.
 */
static int findFreeFrame() {
    for (int f = 0; f < FRAME_COUNT; f++) {
        if (!g_frames[f].occupied) return f;
    }
    return -1;
}

// ─── chooseVictimFrameFIFO ────────────────────────────────────────────────────
/*
 * FIFO page replacement: selects the oldest occupied frame for eviction.
 * g_fifoFrames is a queue of frame indices in the order they were loaded.
 * The front of the queue is always the oldest — we pop and return it.
 *
 * Stale entries (frames freed by a terminated process) are skipped: they
 * remain in the FIFO queue until we encounter them here, at which point we
 * just discard them and keep looking.
 *
 * Returns the frame index to evict, or -1 if no occupied frame is found
 * (this should never happen under normal operation but guards against
 * bookkeeping errors to avoid silently corrupting frame 0).
 */
static int chooseVictimFrameFIFO() {
    while (!g_fifoFrames.empty()) {
        int f = g_fifoFrames.front();
        g_fifoFrames.pop();
        // Skip stale entries left behind by processes that already exited
        if (f >= 0 && f < FRAME_COUNT && g_frames[f].occupied) {
            return f;   // oldest valid occupied frame — this is our victim
        }
    }
    // FIFO queue exhausted with no occupied frame found; return -1 so the
    // caller can detect the error instead of silently evicting frame 0.
    return -1;
}

// ─── mapPageIntoFrame ─────────────────────────────────────────────────────────
/*
 * Loads logical page `page` of process `idx` into physical frame `frame`.
 * If the frame is currently occupied by another page (an evicted page), that
 * old owner's page table entry is invalidated first. Then the frame's metadata
 * is updated to reflect the new owner, the dirty bit is set to 0 (clean load)
 * or 1 (write fault), and the process's page table is updated so future
 * accesses to this page will find it in `frame`. Finally, the frame is pushed
 * onto the FIFO eviction queue to record its load order.
 */
static void mapPageIntoFrame(int idx, int page, int frame, bool isWrite) {
    // Invalidate the evicted page's entry in its owner's page table
    int oldProc = g_frames[frame].processIndex;
    int oldPage = g_frames[frame].page;
    if (oldProc >= 0 && oldProc < TABLE_SIZE &&
        oldPage >= 0 && oldPage < PAGES_PER_PROCESS) {
        g_table[oldProc].pageTable[oldPage] = -1;   // that page is no longer in memory
    }

    // Load the new page into this frame
    g_frames[frame].occupied     = true;
    g_frames[frame].dirty        = isWrite ? 1 : 0; // write faults mark the frame dirty immediately
    g_frames[frame].processIndex = idx;
    g_frames[frame].page         = page;

    // Update the process's page table so it can find this page
    g_table[idx].pageTable[page] = frame;

    // Record this frame's load time in the FIFO queue for future eviction ordering
    g_fifoFrames.push(frame);
}

// ─── sendAckToProcess ─────────────────────────────────────────────────────────
/*
 * Sends an acknowledgement message to worker process `idx` telling it that
 * its last memory request has been completed (either a page hit or a page
 * fault that has now been resolved). The worker is blocked on msgrcv waiting
 * for this message; receiving it lets it loop back and make its next request.
 */
static void sendAckToProcess(int idx) {
    Message ack;
    memset(&ack, 0, sizeof(ack));
    ack.mtype   = g_table[idx].pid;  // mtype = PID so worker receives it with msgrcv(pid)
    ack.index   = idx;
    ack.action  = 999;               // 999 is the dispatch/ack token
    ack.granted = 1;                 // 1 = access granted / completed

    if (msgsnd(g_msgid, &ack, sizeof(Message) - sizeof(long), 0) == -1) {
        logBoth("OSS: failed sending ACK to P%d: %s\n",
                g_table[idx].localPid, strerror(errno));
    }
}

// ─── printBlockedList ────────────────────────────────────────────────────────
/*
 * Prints the current disk I/O wait queue — all processes that are blocked
 * on a page fault, along with the address they faulted on, whether it was a
 * read or write, and the sim-clock time at which the I/O will complete.
 * Called inside printProcessTable() every 0.5 sim-seconds.
 */
static void printBlockedList() {
    logBoth("Blocked queue [");
    for (const auto& req : g_blockedQueue) {
        if (req.processIndex >= 0 &&
            req.processIndex < TABLE_SIZE &&
            g_table[req.processIndex].occupied) {
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

// ─── printFrameTable ─────────────────────────────────────────────────────────
/*
 * Prints all 64 physical frames showing:
 *   - Whether the frame is occupied
 *   - The dirty bit (1 = written to since load, 0 = clean)
 *   - Which process (by local PID) owns the frame (-1 if free)
 *   - Which logical page is stored in the frame (-1 if free)
 * Output matches the format required by the spec.
 */
static void printFrameTable() {
    logBoth("Frame Table:\n");
    logBoth("Frame Occupied DirtyBit Process Page\n");
    for (int f = 0; f < FRAME_COUNT; f++) {
        logBoth("Frame %-2d: %-3s %-8d %-7d %-4d\n",
                f,
                g_frames[f].occupied ? "Yes" : "No",
                g_frames[f].dirty,
                g_frames[f].processIndex >= 0
                    ? g_table[g_frames[f].processIndex].localPid : -1,
                g_frames[f].page);
    }
}

// ─── printPageTables ─────────────────────────────────────────────────────────
/*
 * For every currently active process, prints its full 16-entry page table.
 * Each entry shows the physical frame number holding that logical page,
 * or -1 if the page is not currently in memory.
 * Format:  P1 page table: [ -1 -1 4 18 22 ... ]
 */
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

// ─── printProcessTable ───────────────────────────────────────────────────────
/*
 * Periodic status dump called every 0.5 simulated seconds.
 * Prints in order:
 *   1. Current sim clock and oss PID
 *   2. Full PCB table with per-process stats
 *   3. Blocked (page-fault) queue
 *   4. "Current memory layout" header + frame table + page tables
 * This gives the grader a complete snapshot of the system state at each
 * half-second mark.
 */
static void printProcessTable() {
    logBoth("\nOSS PID:%d SysClockS:%u SysClockNano:%u\n",
            getpid(), g_clk->seconds, g_clk->nanoseconds);
    logBoth("Process Table:\n");
    logBoth("Entry Occupied PID      LocalPID StartS StartN   EndS EndN     Blocked Accesses AvgAccess(ns)\n");

    for (int i = 0; i < TABLE_SIZE; i++) {
        LocalPCB& p = g_table[i];
        unsigned long avg = (p.memoryAccesses == 0)
                            ? 0
                            : p.totalAccessTimeNs / p.memoryAccesses;
        logBoth("%-5d %-8d %-8d %-8d %-6u %-8u %-4u %-8u %-7d %-8lu %-12lu\n",
                i, p.occupied ? 1 : 0, (int)p.pid, p.localPid,
                p.startSeconds, p.startNano,
                p.endSeconds,   p.endNano,
                p.blocked ? 1 : 0,
                p.memoryAccesses, avg);
    }

    printBlockedList();

    // Print the memory layout section (frame table + page tables)
    // Header matches the spec's example output format
    logBoth("\nCurrent memory layout at time %u:%u is:\n",
            g_clk->seconds, g_clk->nanoseconds);
    printFrameTable();
    printPageTables();
    logBoth("\n");
}

// ─── allActiveProcessesBlocked ───────────────────────────────────────────────
/*
 * Returns true if every currently active process is blocked (i.e. waiting
 * for disk I/O) AND there is at least one pending blocked request.
 * Used by advanceClockIfSoftBlocked() to detect the soft-deadlock condition
 * where no process can run because everyone is waiting on page faults.
 */
static bool allActiveProcessesBlocked() {
    bool sawActive = false;
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (g_table[i].occupied) {
            sawActive = true;
            if (!g_table[i].blocked) return false;  // at least one is runnable
        }
    }
    // Return true only if we saw at least one process AND all were blocked
    return sawActive && !g_blockedQueue.empty();
}

// ─── serviceReadyBlockedRequests ─────────────────────────────────────────────
/*
 * Checks the disk I/O wait queue for requests whose ready time has been
 * reached by the current sim clock, and services them one by one.
 *
 * For each ready request:
 *   1. Find a free frame, or evict the oldest via FIFO if memory is full.
 *   2. If the evicted frame was dirty, add extra write-back time to the clock.
 *   3. Load the faulted page into the chosen frame (mapPageIntoFrame).
 *   4. Unblock the process and send it an ack so it can continue.
 *   5. Remove the request from the queue.
 *
 * Loops until no more ready requests remain (handles the case where a
 * clock jump or dirty-bit penalty pushes multiple requests past their
 * ready times in one call).
 *
 * Note: because the disk queue is sequential (each fault's ready time is
 * computed as max(now, tail_ready) + 14ms), only one request will typically
 * become ready per 14 ms interval.
 */
static void serviceReadyBlockedRequests() {
    bool changed = true;
    while (changed) {
        changed = false;
        for (auto it = g_blockedQueue.begin(); it != g_blockedQueue.end(); ) {
            int idx = it->processIndex;

            // Stale entry: process already exited — just remove it
            if (idx < 0 || idx >= TABLE_SIZE || !g_table[idx].occupied) {
                it = g_blockedQueue.erase(it);
                changed = true;
                continue;
            }

            // Check if this request's disk I/O has completed
            if (timeGTE(g_clk->seconds, g_clk->nanoseconds,
                        it->readySeconds, it->readyNano)) {

                int page    = it->address / PAGE_SIZE;
                bool isWrite = (it->action == 2);

                // Try to find a free frame first; if none, evict via FIFO
                int frame = findFreeFrame();
                bool dirtyVictim = false;

                if (frame == -1) {
                    // Memory is full — FIFO eviction
                    frame = chooseVictimFrameFIFO();
                    // Guard: if FIFO returned -1, bookkeeping is broken; defer
                    if (frame == -1) {
                        logBoth("oss: ERROR no victim frame for P%d page %d; deferring\n",
                                g_table[idx].localPid, page);
                        ++it;
                        continue;
                    }
                    dirtyVictim = (g_frames[frame].dirty != 0);
                    logBoth("oss: Clearing frame %d and swapping in P%d page %d\n",
                            frame, g_table[idx].localPid, page);
                    // Dirty frame requires extra write-back time before new page loads
                    if (dirtyVictim) {
                        logBoth("oss: Dirty bit of frame %d set, adding additional time to the clock\n",
                                frame);
                        addToClock(DIRTY_EXTRA_NS);
                        g_totalMemoryAccessTimeNs      += DIRTY_EXTRA_NS;
                        g_table[idx].totalAccessTimeNs += DIRTY_EXTRA_NS;
                    }
                } else {
                    logBoth("oss: Using free frame %d and swapping in P%d page %d\n",
                            frame, g_table[idx].localPid, page);
                }

                // Load the page into the chosen frame and update all tables
                mapPageIntoFrame(idx, page, frame, isWrite);

                // Unblock the process
                g_table[idx].blocked       = 0;
                g_table[idx].blockedAddress = -1;
                g_table[idx].blockedAction  = 0;

                logBoth("oss: Indicating to P%d that %s has happened to address %05d at time %u:%u\n",
                        g_table[idx].localPid,
                        isWrite ? "write" : "read",
                        it->address,
                        g_clk->seconds, g_clk->nanoseconds);

                // Send ack so the worker can make its next memory request
                sendAckToProcess(idx);
                it = g_blockedQueue.erase(it);
                changed = true;
            } else {
                ++it;
            }
        }
    }
}

// ─── advanceClockIfSoftBlocked ────────────────────────────────────────────────
/*
 * Detects and resolves the "soft deadlock" condition: all active processes are
 * blocked waiting for disk I/O, so no process can be dispatched and the clock
 * will never advance on its own.
 *
 * Resolution: jump the sim clock forward to the earliest ready time in the
 * blocked queue. This lets serviceReadyBlockedRequests() immediately unblock
 * that process, breaking the deadlock. The spec explicitly requires this:
 * "oss should periodically check if all the processes are queued for device
 * and if so, advance the clock to fulfill the request at the head."
 */
static void advanceClockIfSoftBlocked() {
    if (!allActiveProcessesBlocked()) return;   // at least one runnable process; no problem

    // Find the minimum (earliest) ready time across all pending requests
    unsigned int nextS  = g_blockedQueue[0].readySeconds;
    unsigned int nextNS = g_blockedQueue[0].readyNano;
    for (const auto& req : g_blockedQueue) {
        if (!timeGTE(req.readySeconds, req.readyNano, nextS, nextNS)) {
            nextS  = req.readySeconds;
            nextNS = req.readyNano;
        }
    }

    // Only advance the clock if it hasn't reached that point yet
    if (!timeGTE(g_clk->seconds, g_clk->nanoseconds, nextS, nextNS)) {
        logBoth("oss: All active processes blocked; advancing clock to %u:%u to prevent soft deadlock\n",
                nextS, nextNS);
        g_clk->seconds     = nextS;
        g_clk->nanoseconds = nextNS;
    }
}

// ─── blockForPageFault ───────────────────────────────────────────────────────
/*
 * Called when a worker requests a page that is not currently in memory.
 * Marks the process as blocked, creates a BlockedRequest entry with the
 * correct disk-ready timestamp, and pushes it onto the I/O wait queue.
 *
 * Disk queue timing (sequential device):
 *   readyTime = max(now, tail_readyTime) + DISK_ACCESS_NS
 *
 * Using max() ensures new faults always queue BEHIND the current tail even
 * if the clock has jumped ahead of the tail (e.g. after a soft-deadlock
 * clock advance). This correctly models a single disk serving one request
 * at a time: fault1 ready at T+14ms, fault2 at T+28ms, fault3 at T+42ms.
 *
 * The process is NOT sent an ack here — it stays blocked on msgrcv until
 * serviceReadyBlockedRequests() calls sendAckToProcess() once the I/O is done.
 */
static void blockForPageFault(int idx, int address, int action) {
    bool isWrite = (action == 2);
    int  page    = address / PAGE_SIZE;

    g_totalPageFaults++;
    g_table[idx].blocked        = 1;
    g_table[idx].blockedAddress = address;
    g_table[idx].blockedAction  = action;

    BlockedRequest req;
    req.processIndex = idx;
    req.address      = address;
    req.action       = action;

    // Sequential disk: queue behind the last pending request
    // readyTime = max(now, tail_ready) + 14 ms
    unsigned int baseS  = g_clk->seconds;
    unsigned int baseNS = g_clk->nanoseconds;
    if (!g_blockedQueue.empty()) {
        const BlockedRequest& tail = g_blockedQueue.back();
        // If the tail's ready time is still in the future, start from there
        if (timeGTE(tail.readySeconds, tail.readyNano, baseS, baseNS)) {
            baseS  = tail.readySeconds;
            baseNS = tail.readyNano;
        }
    }
    req.readySeconds = baseS;
    req.readyNano    = baseNS + DISK_ACCESS_NS;
    normalizeTime(req.readySeconds, req.readyNano);

    g_blockedQueue.push_back(req);

    // Account for the disk I/O time in this process's access time stats
    g_totalMemoryAccessTimeNs      += DISK_ACCESS_NS;
    g_table[idx].totalAccessTimeNs += DISK_ACCESS_NS;

    logBoth("oss: Address %05d from P%d page %d is not in a frame, pagefault at time %u:%u\n",
            address, g_table[idx].localPid, page, g_clk->seconds, g_clk->nanoseconds);
    logBoth("oss: Queuing disk %s for P%d until time %u:%u\n",
            isWrite ? "write" : "read",
            g_table[idx].localPid,
            req.readySeconds, req.readyNano);
}

// ─── handleMemoryRequest ─────────────────────────────────────────────────────
/*
 * Processes a single memory read or write request from a worker.
 * Extracts the page number from the address (address / PAGE_SIZE), then:
 *
 *   Page HIT  (page already in a frame):
 *     - On write: set the frame's dirty bit to 1
 *     - Add 100 ns to the sim clock
 *     - Immediately send ack back to the worker
 *
 *   Page FAULT (page not in memory):
 *     - Call blockForPageFault() to queue the disk I/O request
 *     - The worker stays blocked; it will be unblocked later by
 *       serviceReadyBlockedRequests() when the 14 ms delay expires
 *
 * Also validates the address range and updates global/per-process counters.
 */
static void handleMemoryRequest(const Message& reply) {
    int idx     = reply.index;
    int action  = reply.action;
    int address = reply.address;

    if (idx < 0 || idx >= TABLE_SIZE || !g_table[idx].occupied) return;

    bool isWrite = (action == 2);
    bool isRead  = (action == 1);
    if (!isRead && !isWrite) return;   // ignore unexpected action values

    // Validate that the requested address is within the process's logical space
    if (address < 0 || address >= PAGES_PER_PROCESS * PAGE_SIZE) {
        logBoth("oss: P%d made invalid address request %d; ignoring\n",
                g_table[idx].localPid, address);
        sendAckToProcess(idx);   // unblock the worker so it doesn't hang
        return;
    }

    int page = address / PAGE_SIZE;  // extract logical page number

    // Update global and per-process counters
    g_totalRequests++;
    g_table[idx].memoryAccesses++;
    if (isWrite) g_totalWrites++; else g_totalReads++;

    logBoth("oss: P%d requesting %s of address %05d at time %u:%u\n",
            g_table[idx].localPid,
            isWrite ? "write" : "read",
            address, g_clk->seconds, g_clk->nanoseconds);

    // Look up the page in this process's page table
    int frame = g_table[idx].pageTable[page];

    if (frame >= 0 && frame < FRAME_COUNT && g_frames[frame].occupied) {
        // ── PAGE HIT ──────────────────────────────────────────────────
        if (isWrite) g_frames[frame].dirty = 1;   // mark dirty on write
        addToClock(MEMORY_ACCESS_NS);              // 100 ns access time
        g_totalMemoryAccessTimeNs      += MEMORY_ACCESS_NS;
        g_table[idx].totalAccessTimeNs += MEMORY_ACCESS_NS;

        logBoth("oss: Address %05d in frame %d, %s data for P%d at time %u:%u\n",
                address, frame,
                isWrite ? "writing" : "giving",
                g_table[idx].localPid,
                g_clk->seconds, g_clk->nanoseconds);

        sendAckToProcess(idx);   // worker can immediately proceed
    } else {
        // ── PAGE FAULT ────────────────────────────────────────────────
        blockForPageFault(idx, address, action);
        // Worker stays blocked; ack sent later by serviceReadyBlockedRequests()
    }
}

// ─── terminateProcess ────────────────────────────────────────────────────────
/*
 * Called when a worker sends ACTION_TERMINATE (action == 0), signalling that
 * its simulated time limit has been reached and it is exiting voluntarily.
 * Logs the process's effective memory access time, frees its frames,
 * reaps the child with WNOHANG (non-blocking so oss doesn't stall if the
 * worker hasn't fully exited yet), and clears its PCB slot.
 */
static void terminateProcess(int idx, int& activeChildren) {
    if (idx < 0 || idx >= TABLE_SIZE || !g_table[idx].occupied) return;

    // Compute and log per-process average memory access time
    unsigned long avg = (g_table[idx].memoryAccesses == 0)
        ? 0
        : g_table[idx].totalAccessTimeNs / g_table[idx].memoryAccesses;

    logBoth("oss: Process P%d terminating at time %u:%u. "
            "Effective memory access time: %lu ns\n",
            g_table[idx].localPid,
            g_clk->seconds, g_clk->nanoseconds, avg);

    releaseProcessFrames(idx);
    // WNOHANG: don't block if child hasn't exited yet; reapExitedChildren()
    // will catch it on a future iteration
    waitpid(g_table[idx].pid, nullptr, WNOHANG);
    clearPCB(idx);
    activeChildren--;
}

// ─── reapExitedChildren ───────────────────────────────────────────────────────
/*
 * Non-blocking sweep for child processes that have exited on their own
 * (e.g. killed by a signal, or exited due to an error).
 * Uses waitpid(-1, WNOHANG) in a loop to collect all available zombies
 * without blocking. For each reaped child, releases its frames and PCB slot.
 * This prevents zombie accumulation and keeps the PCB table consistent.
 */
static void reapExitedChildren(int& activeChildren) {
    while (true) {
        int status = 0;
        pid_t dead = waitpid(-1, &status, WNOHANG);
        if (dead <= 0) break;   // no more exited children right now

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

// ─── reapExpiredBlockedProcesses ─────────────────────────────────────────────
/*
 * Edge case handler: a process that is currently blocked waiting for disk I/O
 * might also have exceeded its simulated time limit. If that happens, we kill
 * it rather than waiting for the I/O to complete, so it doesn't hold a slot
 * and a pending queue entry indefinitely.
 * releaseProcessFrames() also removes its entry from the blocked queue.
 */
static void reapExpiredBlockedProcesses(int& activeChildren) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (g_table[i].occupied && g_table[i].blocked &&
            timeGTE(g_clk->seconds, g_clk->nanoseconds,
                    g_table[i].endSeconds, g_table[i].endNano)) {

            logBoth("oss: Terminating blocked P%d because its time expired at %u:%u\n",
                    g_table[i].localPid, g_clk->seconds, g_clk->nanoseconds);

            kill(g_table[i].pid, SIGTERM);
            waitpid(g_table[i].pid, nullptr, 0);   // safe to block here; SIGTERM is already sent
            releaseProcessFrames(i);
            clearPCB(i);
            activeChildren--;
        }
    }
}

// ─── main ────────────────────────────────────────────────────────────────────
/*
 * Entry point for the OS simulator.
 *
 * Startup sequence:
 *   1. Install signal handlers (SIGINT + SIGALRM watchdog)
 *   2. Parse CLI arguments
 *   3. Create shared memory for the SimClock
 *   4. Create the System V message queue
 *   5. Initialize PCB table, frame table, FIFO queue
 *
 * Main loop (runs until all n workers have been launched AND exited):
 *   Each iteration:
 *     a. Reap any unexpectedly exited children
 *     b. Service any disk I/O requests that are now ready
 *     c. Resolve soft deadlock if all processes are blocked
 *     d. Launch a new worker if conditions allow
 *     e. Advance the sim clock by CLOCK_INCREMENT_NS
 *     f. Kill any blocked processes whose time limit has expired
 *     g. Dispatch one runnable process; receive and handle its reply
 *     h. Advance the clock again
 *     i. Print the periodic status report every 0.5 sim-seconds
 *
 * Shutdown: print summary statistics, call cleanup().
 */
int main(int argc, char* argv[]) {
    // Install handlers so we clean up IPC on Ctrl-C or the 5-second alarm
    signal(SIGINT,  signal_handler);
    signal(SIGALRM, signal_handler);
    alarm(5);  // hard real-time kill after 5 seconds per spec; prevents hangs

    srand((unsigned int)(time(nullptr) ^ getpid()));

    // ── Default CLI parameter values ──────────────────────────────────────
    int    n        = 1;       // total processes to launch
    int    s        = 1;       // max simultaneously active
    double t        = 1.0;     // simulated lifetime per worker (seconds)
    double interval = 0.1;     // simulated interval between launches (seconds)
    string logFilename = "log.txt";

    // ── Parse command-line arguments ──────────────────────────────────────
    int opt;
    while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
        switch (opt) {
            case 'h':
                printHelp(argv[0]);
                return 0;
            case 'n':
                n = atoi(optarg);
                if (n <= 0)  { cerr << "Error: -n must be > 0\n";   return 1; }
                if (n > 80)  { cerr << "Error: -n must be <= 80\n";  return 1; }
                break;
            case 's':
                s = atoi(optarg);
                if (s <= 0)  { cerr << "Error: -s must be > 0\n";   return 1; }
                if (s > 15)  { cerr << "Error: -s must be <= 15\n";  return 1; }
                break;
            case 't':
                t = atof(optarg);
                if (t <= 0.0) { cerr << "Error: -t must be > 0\n"; return 1; }
                break;
            case 'i':
                interval = atof(optarg);
                if (interval < 0.0) { cerr << "Error: -i must be >= 0\n"; return 1; }
                break;
            case 'f':
                logFilename = optarg;
                break;
            default:
                printHelp(argv[0]);
                return 1;
        }
    }

    // Clamp s so it never exceeds n or the hard maximum
    if (s > n)              s = n;
    if (s > MAX_ACTIVE_PROCS) s = MAX_ACTIVE_PROCS;

    // ── Open log file ─────────────────────────────────────────────────────
    g_logFile = fopen(logFilename.c_str(), "w");
    if (!g_logFile) {
        cerr << "OSS: failed to open log file: " << logFilename << "\n";
        return 1;
    }

    // ── Create shared memory for the simulated clock ──────────────────────
    // ftok generates a consistent IPC key from the current directory + 'C'
    key_t shmKey = ftok(".", 'C');
    if (shmKey == -1) {
        cerr << "OSS: ftok shared memory failed: " << strerror(errno) << "\n";
        cleanup(); return 1;
    }
    g_shmid = shmget(shmKey, sizeof(SimClock), 0666 | IPC_CREAT);
    if (g_shmid == -1) {
        cerr << "OSS: shmget failed: " << strerror(errno) << "\n";
        cleanup(); return 1;
    }
    g_clk = (SimClock*)shmat(g_shmid, nullptr, 0);
    if (g_clk == (SimClock*)-1) {
        cerr << "OSS: shmat failed: " << strerror(errno) << "\n";
        cleanup(); return 1;
    }
    g_clk->seconds     = 0;   // start the simulated clock at time 0:0
    g_clk->nanoseconds = 0;

    // ── Create System V message queue ─────────────────────────────────────
    key_t msgKey = ftok(".", 'Q');
    if (msgKey == -1) {
        cerr << "OSS: ftok message queue failed: " << strerror(errno) << "\n";
        cleanup(); return 1;
    }
    g_msgid = msgget(msgKey, 0666 | IPC_CREAT);
    if (g_msgid == -1) {
        cerr << "OSS: msgget failed: " << strerror(errno) << "\n";
        cleanup(); return 1;
    }

    // ── Initialize all tables ─────────────────────────────────────────────
    initTables();

    // ── Main loop bookkeeping variables ───────────────────────────────────
    int launched       = 0;    // total workers launched so far
    int activeChildren = 0;    // currently running workers
    int nextLocalPid   = 1;    // next logical PID to assign (P1, P2, ...)

    // Sim-clock timestamps for scheduled events
    unsigned int nextLaunchS  = 0, nextLaunchNS  = 0;          // when to launch next child
    unsigned int nextPrintS   = 0, nextPrintNS   = 500000000U; // when to print next report (0.5s)

    // Pre-compute launch interval in sim-clock units
    unsigned int intervalSec  = secondsPart(interval);
    unsigned int intervalNano = nanosPart(interval);

    // Real-time start for the 5-second new-launch cutoff
    time_t realStart = time(nullptr);

    // ── Main simulation loop ──────────────────────────────────────────────
    // Keep running as long as there are more workers to launch (within 5 real
    // seconds) OR there are still active children that haven't finished yet.
    while ((launched < n && difftime(time(nullptr), realStart) < 5.0)
           || activeChildren > 0) {

        // (a) Collect any children that have already exited on their own
        reapExitedChildren(activeChildren);

        // (b) Unblock any page-fault requests whose 14 ms I/O delay has elapsed
        serviceReadyBlockedRequests();

        // (c) If everyone is blocked, jump the clock to the nearest unblock time
        advanceClockIfSoftBlocked();

        // Run serviceReadyBlockedRequests again in case the clock jump
        // immediately made one or more requests ready
        serviceReadyBlockedRequests();

        // (d) Launch a new worker if: under the total limit, within 5 real sec,
        //     under the concurrency limit, and past the next launch timestamp
        if (launched < n &&
            difftime(time(nullptr), realStart) < 5.0 &&
            activeChildren < s &&
            activeChildren < MAX_ACTIVE_PROCS &&
            timeGTE(g_clk->seconds, g_clk->nanoseconds, nextLaunchS, nextLaunchNS)) {

            int slot = findFreeSlot();
            if (slot != -1) {
                // Compute this worker's simulated deadline
                unsigned int endS  = g_clk->seconds  + (unsigned int)t;
                unsigned int endNS = g_clk->nanoseconds + nanosPart(t);
                normalizeTime(endS, endNS);

                pid_t child = fork();
                if (child == 0) {
                    // ── Child process: exec the worker ──────────────────
                    string idxStr  = to_string(slot);
                    string secStr  = to_string(endS);
                    string nanoStr = to_string(endNS);
                    execl("./worker", "worker",
                          idxStr.c_str(), secStr.c_str(), nanoStr.c_str(),
                          (char*)nullptr);
                    cerr << "OSS: execl failed: " << strerror(errno) << "\n";
                    _exit(1);

                } else if (child > 0) {
                    // ── Parent: fill in the PCB slot ────────────────────
                    clearPCB(slot);
                    g_table[slot].occupied     = 1;
                    g_table[slot].pid          = child;
                    g_table[slot].localPid     = nextLocalPid++;
                    g_table[slot].startSeconds = g_clk->seconds;
                    g_table[slot].startNano    = g_clk->nanoseconds;
                    g_table[slot].endSeconds   = endS;
                    g_table[slot].endNano      = endNS;
                    g_table[slot].blocked      = 0;

                    launched++;
                    activeChildren++;

                    // Schedule next launch at current time + interval
                    unsigned int tempS  = g_clk->seconds  + intervalSec;
                    unsigned int tempNS = g_clk->nanoseconds + intervalNano;
                    normalizeTime(tempS, tempNS);
                    nextLaunchS  = tempS;
                    nextLaunchNS = tempNS;

                    logBoth("OSS: Generating process with local PID %d in slot %d"
                            " at time %u:%u\n",
                            g_table[slot].localPid, slot,
                            g_clk->seconds, g_clk->nanoseconds);
                } else {
                    cerr << "OSS: fork failed: " << strerror(errno) << "\n";
                }
            }
        }

        // (e) Advance clock by one small tick (1 µs) to keep time moving
        addToClock(CLOCK_INCREMENT_NS);

        // (f) Kill any blocked processes that have exceeded their time limit
        reapExpiredBlockedProcesses(activeChildren);

        // (g) Dispatch one runnable process and handle its response
        int picked = pickRunnableProcess();
        if (picked != -1) {

            // Send dispatch token — worker wakes up from its msgrcv
            Message msg;
            memset(&msg, 0, sizeof(msg));
            msg.mtype   = g_table[picked].pid;  // targeted to this specific worker
            msg.index   = picked;
            msg.action  = 999;  // dispatch/ack token
            msg.granted = 1;

            if (msgsnd(g_msgid, &msg, sizeof(Message) - sizeof(long), 0) == -1) {
                cerr << "OSS: msgsnd dispatch failed: " << strerror(errno) << "\n";
                cleanup(); return 1;
            }

            // Wait for the worker's reply (mtype=1 means oss-directed reply)
            Message reply;
            memset(&reply, 0, sizeof(reply));
            if (msgrcv(g_msgid, &reply, sizeof(Message) - sizeof(long), 1, 0) == -1) {
                if (errno == EINTR) continue;   // interrupted by signal; retry
                cerr << "OSS: msgrcv failed: " << strerror(errno) << "\n";
                cleanup(); return 1;
            }

            if (reply.action == ACTION_TERMINATE) {
                // Worker has reached its time limit and is exiting
                terminateProcess(reply.index, activeChildren);
            } else {
                // Worker is making a memory read or write request
                handleMemoryRequest(reply);
            }
        }

        // (h) Second clock tick after handling the process response
        addToClock(CLOCK_INCREMENT_NS);

        // (i) Periodic status report every 0.5 simulated seconds
        if (timeGTE(g_clk->seconds, g_clk->nanoseconds, nextPrintS, nextPrintNS)) {
            printProcessTable();
            // Schedule the next print 0.5 sim-seconds from now
            nextPrintNS = g_clk->nanoseconds + 500000000U;
            nextPrintS  = g_clk->seconds;
            normalizeTime(nextPrintS, nextPrintNS);
        }
    }

    // ── Final summary ─────────────────────────────────────────────────────
    logBoth("\nOSS Summary:\n");
    logBoth("Total memory requests: %lu\n", g_totalRequests);
    logBoth("Total reads:           %lu\n", g_totalReads);
    logBoth("Total writes:          %lu\n", g_totalWrites);
    logBoth("Total page faults:     %lu\n", g_totalPageFaults);

    double faultPct = (g_totalRequests > 0)
        ? ((double)g_totalPageFaults / (double)g_totalRequests) * 100.0
        : 0.0;
    logBoth("Page fault rate:       %.2f%%\n", faultPct);

    // Average effective memory access time across all requests
    unsigned long avgAccess = (g_totalRequests == 0)
        ? 0
        : g_totalMemoryAccessTimeNs / g_totalRequests;
    logBoth("Avg effective access time: %lu ns\n", avgAccess);

    cleanup();
    return 0;
}
