#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <signal.h>

#include "data_logger.h"

int do_run;

void signal_handler(int signo) {
	printf("Signal detected\n");
	do_run=0;
}

int main(int argc, char *argv[]) {
	int mq,retval,size,i;
	key_t key = ftok("/usr/bin/data_logger",0);
	dummy_msg_t data;
	stream_update_msg_t stream_msg;

	mq=msgget(key,0666|IPC_CREAT);
	if(mq==-1){
		perror("Could not open message queue");
		return -1;
	}

	signal(SIGINT,signal_handler);
	
	stream_msg.mtype=1;
	sprintf(stream_msg.guid,"TestDevice");
	sprintf(stream_msg.stream,"TestStream");
	stream_msg.value=1.23;

	do_run=1;
	size=sizeof(stream_msg);
	for(i=0;i<1e3 && do_run;i++){
	printf("Sending %d bytes: %d\n",size,i);
	stream_msg.timestamp=getTimestampMs(NULL);
	stream_msg.value=1.0f * i;
	retval=msgsnd(mq,&stream_msg,sizeof(stream_msg)-sizeof(long),0);
	printf("Retval: %d\n",retval);
	}
	

}
