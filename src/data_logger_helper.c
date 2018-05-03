#include <stdio.h>
#include <string.h>
#include "data_logger.h"
#include "debug.h"

int data_logger_mq_init(void){
  struct msqid_ds mq_info;
	key_t key = 0x54252400;
	int mq=msgget(key,0666|IPC_CREAT);
	if(mq==-1){
		perror("Could not open message queue");
		return -1;
	}

  if(msgctl(mq, IPC_STAT, &mq_info)!=0){
    perror("Could not get status for message queue");
    return -1;
  }
  
  if(mq_info.msg_qbytes<512*1024) {
    printf("Maximum bytes in queue: %d\n",mq_info.msg_qbytes);
    mq_info.msg_qbytes=1024*1024;

    if(msgctl,mq,IPC_SET, &mq_info){
      perror("Could not set new size for message queue");
      return -1;
    }
  }
  
	return mq;
}

int log_double(int mq, char *guid, char *stream, long long timestamp, double value){

	char device_name[1024];
	char data[2048];
	char timebuf[100];
	stream_update_msg_t stream_msg;

	

	debug_printf("Trying to send double value: %s -> %s -> %lld -> %f\n",guid,stream,timestamp,value);

	if(guid==NULL || strlen(guid)==0){
		print_error("Missing guid\n");
		return -1;
	}

	if(stream==NULL || strlen(stream)==0){
		print_error("Missing stream name\n");
		return -1;
	}

	
	getRFC3339(timestamp,timebuf);
	stream_msg.mtype=1;
	sprintf(stream_msg.guid,guid);
	sprintf(stream_msg.stream,stream);
	stream_msg.value=value;
	if(timestamp!=-1){
		stream_msg.timestamp=timestamp;
	}else{
		stream_msg.timestamp=getTimestampMs(NULL);
	}

	if(msgsnd(mq,&stream_msg,sizeof(stream_msg)-sizeof(long),0)){
		perror("Could not send log value");
		return -1;
	}

	return 0;

}


