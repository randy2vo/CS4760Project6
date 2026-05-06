#include <iostream>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <ctime>
#include "shared.h"

using namespace std;

static bool reached(unsigned int s,   unsigned int ns,
                    unsigned int endS, unsigned int endNS) {
    return (s > endS) || (s == endS && ns >= endNS);
}

int main(int argc, char* argv[]) {

    // ── Argument validation ───────────────────────────────────────────────
    // oss always passes exactly three arguments; anything else is a bug
    if (argc != 4) {
        cerr << "Usage: ./worker <index> <endSec> <endNano>\n";
        return 1;
    }

    // argv[1]: PCB slot index — used as the `index` field in every Message
    //          so oss can find this process in its PCB table without a PID lookup
    int index = atoi(argv[1]);

    // argv[2] and argv[3]: simulated clock deadline passed by oss at launch time.
    // The worker terminates once the shared clock reaches or exceeds this value.
    unsigned int endSec  = (unsigned int)strtoul(argv[2], nullptr, 10);
    unsigned int endNano = (unsigned int)strtoul(argv[3], nullptr, 10);

    // ── Attach to shared memory clock ────────────────────────────────────
    // Use the same ftok key ('C') that oss used when it created the segment.
    // The worker only reads the clock; it never writes to it (oss owns it).
    key_t shmKey = ftok(".", 'C');
    if (shmKey == -1) {
        cerr << "Worker ftok shm failed: " << strerror(errno) << "\n";
        return 1;
    }

    // Attach to the existing segment — no IPC_CREAT, since oss already made it
    int shmid = shmget(shmKey, sizeof(SimClock), 0666);
    if (shmid == -1) {
        cerr << "Worker shmget failed: " << strerror(errno) << "\n";
        return 1;
    }

    SimClock* clk = (SimClock*)shmat(shmid, nullptr, 0);
    if (clk == (SimClock*)-1) {
        cerr << "Worker shmat failed: " << strerror(errno) << "\n";
        return 1;
    }

    // ── Connect to the message queue ─────────────────────────────────────
    // Use the same ftok key ('Q') that oss used when it created the queue.
    key_t msgKey = ftok(".", 'Q');
    if (msgKey == -1) {
        cerr << "Worker ftok msg failed: " << strerror(errno) << "\n";
        shmdt(clk);
        return 1;
    }

    // Get the existing queue — no IPC_CREAT, oss already created it
    int msgid = msgget(msgKey, 0666);
    if (msgid == -1) {
        cerr << "Worker msgget failed: " << strerror(errno) << "\n";
        shmdt(clk);
        return 1;
    }

    // ── Seed the RNG ──────────────────────────────────────────────────────
    // XOR time, PID, and slot index so each worker gets a distinct seed
    // even if multiple workers are forked within the same clock second.
    srand((unsigned int)(time(nullptr) ^ getpid() ^ (index << 8)));

    // ── Main request loop ─────────────────────────────────────────────────
    while (true) {

        // ── Step 1: Wait for oss to dispatch this process ─────────────────
        // oss sends a "go" message with mtype = this worker's PID.
        // We use msgrcv with mtype = getpid() so we only receive messages
        // addressed specifically to us (not to other workers).
        // This call blocks until oss picks us via its round-robin scheduler.
        Message msg;
        memset(&msg, 0, sizeof(msg));
        if (msgrcv(msgid, &msg, sizeof(Message) - sizeof(long), getpid(), 0) == -1) {
            // EIDRM: message queue was deleted (oss cleaned up) — exit silently
            // EINTR: interrupted by a signal — also exit cleanly
            if (errno == EIDRM || errno == EINTR) {
                shmdt(clk);
                return 0;
            }
            cerr << "Worker msgrcv failed: " << strerror(errno) << "\n";
            shmdt(clk);
            return 1;
        }

        // Pre-fill the reply fields that are the same for every message type
        Message reply;
        memset(&reply, 0, sizeof(reply));
        reply.mtype   = 1;       // all worker->oss messages use mtype=1
        reply.index   = index;   // lets oss look us up in the PCB table
        reply.granted = -1;      // not used in worker->oss direction
        reply.address = 0;       // will be overwritten for memory requests

        // ── Step 2: Check the simulated clock deadline ────────────────────
        // Read the current clock value from shared memory. If we have reached
        // or passed our assigned deadline, tell oss we are done and exit.
        if (reached(clk->seconds, clk->nanoseconds, endSec, endNano)) {
            reply.action = ACTION_TERMINATE;  // action=0: "I am done"

            if (msgsnd(msgid, &reply, sizeof(Message) - sizeof(long), 0) == -1) {
                cerr << "Worker terminate msgsnd failed: " << strerror(errno) << "\n";
                shmdt(clk);
                return 1;
            }
            break;  // exit the loop; shmdt + return 0 below
        }

        // ── Step 3: Generate a random memory request ──────────────────────
        // Address generation per spec:
        //   page   = random value in [0, PAGES_PER_PROCESS)   → 0..15
        //   offset = random value in [0, PAGE_SIZE)            → 0..1023
        //   address = page * PAGE_SIZE + offset                → 0..16383
        // This covers the entire 16 KB logical address space of the process.
        int page   = rand() % PAGES_PER_PROCESS;
        int offset = rand() % PAGE_SIZE;
        reply.address = page * PAGE_SIZE + offset;

        // Bias toward reads: ~75% read, ~25% write (per spec recommendation)
        int percent   = rand() % 100;
        reply.action  = (percent < 75) ? ACTION_READ : ACTION_WRITE;
        //   ACTION_READ  = 1  →  oss will look up the page, return a page hit
        //                        or block us for a disk read fault
        //   ACTION_WRITE = 2  →  same, but also sets the dirty bit in the frame

        // ── Send the memory request to oss ────────────────────────────────
        if (msgsnd(msgid, &reply, sizeof(Message) - sizeof(long), 0) == -1) {
            cerr << "Worker msgsnd failed: " << strerror(errno) << "\n";
            shmdt(clk);
            return 1;
        }

        // ── Step 4: Wait for oss's acknowledgement ────────────────────────
        // oss will send an ack (mtype = our PID, action = 999, granted = 1)
        // once the memory access is complete:
        //   - Page HIT:   oss sends ack immediately (after 100 ns sim time)
        //   - Page FAULT: oss blocks us, waits for 14 ms disk I/O to finish,
        //                 then sends the ack via serviceReadyBlockedRequests()
        //
        // IMPORTANT: this second msgrcv is mandatory. Without it, the next
        // iteration's msgrcv (Step 1) would consume the ack as a dispatch
        // token and mistake it for a new "go" signal, causing the worker to
        // send a spurious ACTION_TERMINATE on the very next reply.
        Message ack;
        memset(&ack, 0, sizeof(ack));
        if (msgrcv(msgid, &ack, sizeof(Message) - sizeof(long), getpid(), 0) == -1) {
            if (errno == EIDRM || errno == EINTR) {
                shmdt(clk);
                return 0;
            }
            cerr << "Worker ack msgrcv failed: " << strerror(errno) << "\n";
            shmdt(clk);
            return 1;
        }
        // ack.action == 999 and ack.granted == 1 confirms the access is done.
        // We don't need to inspect the ack further — just loop back to Step 1.

    } // end main request loop

shmdt(clk);
return 0;

}
