/* 
 * File:   pub_sub.c
 * Author: james
 *
 * Created on October 27, 2020, 1:40 PM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>

#define HOST "127.0.0.1"
#define PORT 6379

typedef struct PUBSUB {
    redisContext *ctx;          /* for PUBLISH */
    redisAsyncContext *actx;    /* for SUBSCRIBE */
} pubsub;

pubsub *pPUBSUB = NULL;

void quit() {
    redisAsyncDisconnect(pPUBSUB->actx);
}

void publish( const char *channel, const char *message )
{
    char *command = NULL;
    if (!channel || !message)
            return;
    asprintf(&command, "PUBLISH %s %s", channel, message);
    redisCommand(pPUBSUB->ctx, command);
    free(command);
    command = NULL;
}

void action(redisReply *reply) {
    int i = 0;
    switch (reply->type) {
        case REDIS_REPLY_INTEGER:
            publish("testtopic", "EXIT");
            break;

        case REDIS_REPLY_ERROR:
        case REDIS_REPLY_STRING:
            if (!strcasecmp(reply->str, "EXIT")){
                quit();
            }

            break;

        case REDIS_REPLY_ARRAY:
        default:
            if (reply->element) {
                for (i = 0; i < reply->elements; i++) {
                    action(reply->element[i]);
                }
            }
            break;
    }
}

void subCallback(redisAsyncContext *ctx, void *reply, void *priv) {
    if (!ctx || !reply || !priv)
        return;
    redisReply *r = (redisReply *) reply;
    pPUBSUB = (pubsub *)priv;
    action(r);
}

/**
 * \brief Subscribes to a topic and then publishes "exit" command to quit 
 *  
 */
int main(int argc, char** argv) {
    struct timeval timeout = {1, 500000};
    signal(SIGPIPE, SIG_IGN);
    struct event_base *base = event_base_new();

    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, HOST, PORT);
    options.connect_timeout = &timeout;
    
    pubsub *pPUBSUB = calloc(1, sizeof (pubsub));
    
    pPUBSUB->ctx = redisConnectWithTimeout(HOST, PORT, timeout);
    pPUBSUB->actx = redisAsyncConnectWithOptions(&options);
    if (pPUBSUB->actx->err) {
        printf("error: %s\n", pPUBSUB->actx->errstr);
        pPUBSUB->actx = NULL;
        return EXIT_FAILURE;
    }

    redisLibeventAttach(pPUBSUB->actx, base);
    redisAsyncCommand(pPUBSUB->actx, subCallback, pPUBSUB, "SUBSCRIBE testtopic");
    event_base_dispatch(base);
    
    redisFree(pPUBSUB->ctx);
    pPUBSUB->ctx = NULL;
    pPUBSUB->actx = NULL;
    free(pPUBSUB);
    pPUBSUB = NULL;
    
    return (EXIT_SUCCESS);
}
