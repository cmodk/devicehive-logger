#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sqlite3.h>

#include "devicehive.h"

int dh_mq_init(void){
	key_t key = 0x54252401;
	int mq=msgget(key,0666|IPC_CREAT);
	if(mq==-1){
		perror("Could not open message queue");
		return -1;
	}

	return mq;
}

int dhmq_device_register(int mq, char *guid){
	dh_device_register_msg_t msg;

	msg.mtype=1;
	sprintf(msg.guid,"%s",guid);
	return	msgsnd(mq,&msg,sizeof(msg)-sizeof(long),0);
}
