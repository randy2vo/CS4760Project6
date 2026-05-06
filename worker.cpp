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

static bool reached(unsigned int s, unsigned int ns,
                    unsigned int endS, unsigned int endNS) {
    return (s > endS) || (s == endS && ns >= endNS);
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "Usage: ./worker <index> <endSec> <endNano>\n";
        return 1;
    }

    int index = atoi(argv[1]);
    unsigned int endSec = (unsigned int)strtoul(argv[2], nullptr, 10);
    unsigned int endNano = (unsigned int)strtoul(argv[3], nullptr, 10);

    key_t shmKey = ftok(".", 'C');
    if (shmKey == -1) {
        cerr << "Worker ftok shm failed: " << strerror(errno) << "\n";
        return 1;
    }

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

    key_t msgKey = ftok(".", 'Q');
    if (msgKey == -1) {
        cerr << "Worker ftok msg failed: " << strerror(errno) << "\n";
        shmdt(clk);
        return 1;
    }

    int msgid = msgget(msgKey, 0666);
    if (msgid == -1) {
        cerr << "Worker msgget failed: " << strerror(errno) << "\n";
        shmdt(clk);
        return 1;
    }

    srand((unsigned int)(time(nullptr) ^ getpid() ^ (index << 8)));

    while (true) {
        Message msg;
        memset(&msg, 0, sizeof(msg));

        // Wait until oss dispatches/acknowledges this process.
        if (msgrcv(msgid, &msg, sizeof(Message) - sizeof(long), getpid(), 0) == -1) {
            if (errno == EIDRM || errno == EINTR) {
                shmdt(clk);
                return 0;
            }
            cerr << "Worker msgrcv failed: " << strerror(errno) << "\n";
            shmdt(clk);
            return 1;
        }

        Message reply;
        memset(&reply, 0, sizeof(reply));
        reply.mtype = 1;      // oss receives replies on message type 1
        reply.index = index;
        reply.granted = -1;
        reply.address = 0;  // unused on terminate path

        if (reached(clk->seconds, clk->nanoseconds, endSec, endNano)) {
            reply.action = ACTION_TERMINATE;

            if (msgsnd(msgid, &reply, sizeof(Message) - sizeof(long), 0) == -1) {
                cerr << "Worker terminate msgsnd failed: " << strerror(errno) << "\n";
                shmdt(clk);
                return 1;
            }
            break;
        }

        // Generate a random address: page * 1024 + offset.
        int page = rand() % PAGES_PER_PROCESS;
        int offset = rand() % PAGE_SIZE;
        reply.address = page * PAGE_SIZE + offset;

        // Bias requests toward reads. About 75% read, 25% write.
        int percent = rand() % 100;
        reply.action = (percent < 75) ? ACTION_READ : ACTION_WRITE;

        if (msgsnd(msgid, &reply, sizeof(Message) - sizeof(long), 0) == -1) {
            cerr << "Worker msgsnd failed: " << strerror(errno) << "\n";
            shmdt(clk);
            return 1;
        }

        // Wait for oss to acknowledge the memory access. oss sends the ack
        // once the page is in a frame (immediately on hit, after 14ms I/O on
        // fault). Without this explicit receive, the next loop iteration would
        // consume the ack as a dispatch token and send a spurious terminate.
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
        // ack.action == 999 and ack.granted == 1 means access was completed.
    }

    shmdt(clk);
    return 0;
}
