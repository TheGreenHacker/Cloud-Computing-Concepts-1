/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <random>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    
    /* Each node adds itself to its membership list */
    memberNode->memberList.emplace_back(id, port, memberNode->heartbeat, par->getcurrtime());
    log->logNodeAdd(&memberNode->addr, &memberNode->addr);
    memberNode->myPos = memberNode->memberList.begin();

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    Address addr;
    MessageHdr *recv_msg, *send_msg;
    recv_msg = (MessageHdr *) data;
    
    long currtime = par->getcurrtime();
    int id, myid, nMLE;
    short port, myport;
    long heartbeat;
    size_t msgsize;
    //vector<MemberListEntry> upNodeEntries; // entries for nodes that have not been removed
    
    myid = *(int*)(&memberNode->addr.addr);
    myport = *(short*)(&memberNode->addr.addr[4]);
    nMLE = (size - sizeof(MessageHdr))/sizeof(MemberListEntry);
    switch (recv_msg->msgType) {
        /* Introducer received JOINREQ message. */
        case JOINREQ:
            memcpy(&addr.addr, recv_msg + 1, sizeof(addr.addr));
            heartbeat = *(long*)((char *)(recv_msg + 1) + sizeof(addr.addr));
            
            /* Update membership table to contain the new node and log it */
            id = *(int*)(&addr.addr);
            port = *(short*)(&addr.addr[4]);
            
            memberNode->memberList.emplace_back(id, port, heartbeat, currtime);
            memberNode->nnb++;
            log->logNodeAdd(&memberNode->addr, &addr);
        
            /* Prepare JOINREP message containing introducer's membership table */
            msgsize = sizeof(MessageHdr) + (memberNode->nnb + 1) * sizeof(MemberListEntry);
            send_msg = (MessageHdr *) malloc(msgsize * sizeof(char));
            
            send_msg->msgType = JOINREP;
            for (int i = 0; i < memberNode->nnb + 1; i++) {
                memcpy((char *)(send_msg + 1) + i * sizeof(MemberListEntry), &memberNode->memberList[i], sizeof(MemberListEntry));
            }
            
            /* Send requesting node with the JOINREP message containing copy of membership table */
            emulNet->ENsend(&memberNode->addr, &addr, (char *)send_msg, msgsize);
            
            memberNode->myPos = memberNode->memberList.begin();
            free(send_msg);
            break;
        /* New node received JOINREP message. */
        case JOINREP:
            /* Mark node as in the cluster */
            memberNode->inGroup = true;
            
            /* Get rid of original entry for self. Membership table received from introducer node contains updated timestamp for when the node joined the system. */
            memberNode->memberList.clear();
            
            for (int i = 0; i < nMLE; i++) {
                MemberListEntry mle = *(MemberListEntry *)((char *)(recv_msg + 1) + i * sizeof(MemberListEntry));
                memcpy(&addr.addr, &mle.id, sizeof(int));
                memcpy(&addr.addr[4], &mle.port, sizeof(short));
                
                
                memberNode->memberList.emplace_back(mle.id, mle.port, mle.heartbeat, currtime);
                log->logNodeAdd(&memberNode->addr, &addr);
                
                if (mle.id == myid && mle.port == myport) {
                    memberNode->myPos = memberNode->memberList.begin() + i;
                }
            }
            
            memberNode->nnb = nMLE - 1;
            break;
        /* Received another node's membership table; merge with own */
        case GOSSIP:
            for (int i = 0; i < nMLE; i++) {
                MemberListEntry mle = *(MemberListEntry *)((char *)(recv_msg + 1) + i * sizeof(MemberListEntry));
                memcpy(&addr.addr, &mle.id, sizeof(int));
                memcpy(&addr.addr[4], &mle.port, sizeof(short));
                
                auto it = find_if(memberNode->memberList.begin(), memberNode->memberList.end(), [&mle](const MemberListEntry& other_mle) {return mle.id == other_mle.id && mle.port == other_mle.port;});
                
                /* New entry; add it */
                if (it == memberNode->memberList.end()) {
                    memberNode->memberList.emplace_back(mle.id, mle.port, mle.heartbeat, currtime);
                    memberNode->nnb++;
                    log->logNodeAdd(&memberNode->addr, &addr);
                }
                /* Merge with existing entries */
                else if (it->id != myid){
                    if (it->heartbeat != FAILURE) {
                        if (it->heartbeat == mle.heartbeat && currtime - it->timestamp > TFAIL) {
                            it->heartbeat = FAILURE;
                            it->timestamp = currtime;
                        }
                        else if (mle.heartbeat > it->heartbeat){
                            it->heartbeat = mle.heartbeat;
                            it->timestamp = currtime;
                        }
                    }
                }
                else {
                    memberNode->myPos = it;
                }
            }
            break;
        default:
            return false;
    }
    
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    int myid = *(int*)(&memberNode->addr.addr);
    long currtime = par->getcurrtime();
    int nFailedEntries = 0;
    
    /* Check for failed nodes that still haven't responded for TREMOVE */
    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        /* Remove failed node that has timed out */
        if (it->heartbeat == FAILURE) {
            if (currtime - it->timestamp > TREMOVE) {
                Address addr;
                memcpy(&addr.addr, &it->id, sizeof(int));
                memcpy(&addr.addr[4], &it->port, sizeof(short));
                
                it = memberNode->memberList.erase(it);
                memberNode->nnb--;
                log->logNodeRemove(&memberNode->addr, &addr);
            }
            else {
                nFailedEntries++;
                it++;
            }
        }
        else {
            if (it->id == myid) {
                memberNode->myPos = it;
            }
            it++;
        }
    }
    
    /* Increment own heartbeat counter and update timestamp  - nFailedEntries */
    memberNode->heartbeat++;
    memberNode->myPos->setheartbeat(memberNode->heartbeat);
    memberNode->myPos->settimestamp(currtime);
    
    /* Prep msg containing membership table */
    size_t msgsize = sizeof(MessageHdr) + (memberNode->nnb + 1 - nFailedEntries) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = GOSSIP;
    
    vector<int> randIndices; // random indices of entries for nodes to gossip to
    int i = 0;
    for (int j = 0; j < memberNode->nnb + 1; j++) {
        MemberListEntry mle = memberNode->memberList[j];
        
        /* Don't include failed nodes for gossiping to */
        if (mle.heartbeat != FAILURE) {
            memcpy((char *)(msg + 1) + i * sizeof(MemberListEntry), &mle, sizeof(MemberListEntry));
            i++;
            randIndices.push_back(j);
        }
    }
    
    /* Gossip membership table to N random nodes */
    random_shuffle(randIndices.begin(), randIndices.end());
    int size = min<int>(N, randIndices.size());
    for (int i = 0; i < size; i++) {
        Address addr;
        MemberListEntry mle = memberNode->memberList[randIndices[i]];
        
        memcpy(&addr.addr, &mle.id, sizeof(int));
        memcpy(&addr.addr[4], &mle.port, sizeof(short));
        
        /* Handle message drops by re-sending */
        int bytesSent;
        do {
            bytesSent = emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, msgsize);
        } while (bytesSent == 0);
    }
    
    free(msg);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
