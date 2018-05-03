#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <string.h>

#include "data_logger.h"
#include "debug.h"

int do_run;

void signal_handler(int signo) {
	printf("Signal detected\n");
	do_run=0;
}

void db_cleanup(sqlite3 *db){
	sql_exec(db,"DELETE FROM stream_data WHERE is_logged=1;");
}

int main(int argc, char *argv[]) {
	int mq,retval;
	void *data = calloc(1,MQ_MAX_SIZE);
	char query[1024];
	long long ts,te;
	dummy_msg_t *dummy;
	stream_update_msg_t *stream_msg;
	sqlite3 *db;
	sqlite3_stmt *stmt;
	long long last_clean=0;
	long long now;
  struct msqid_ds mq_info;
  int lock_count;

	system("echo 1024000 > /proc/sys/kernel/msgmax");
	system("echo 1024000 > /proc/sys/kernel/msgmnb");

	if((mq=data_logger_mq_init()) == -1){
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
  
		

	if((db=sql_open())==NULL){
		print_fatal("Could not open database\n");
		return -1;
	}

	sprintf(query,"INSERT INTO stream_data VALUES(NULL,?1,?2,?3,?4,?5);");
	if(sqlite3_prepare_v2(db,query,-1,&stmt,0)){
		printf("Failed to execute statement: %s\n", sqlite3_errmsg(db));
		return -1;
	}


	signal(SIGINT,signal_handler);

	do_run=1;
	while(do_run){
		memset(data,0,MQ_MAX_SIZE);
		retval=msgrcv(mq,data,MQ_MAX_SIZE-sizeof(long),0,0);
		if(retval==-1){
			perror("MQ error");
			continue;
		}

		dummy=(dummy_msg_t *)data;
		switch(dummy->mtype){
			case 1:
				debug_printf("Got stream update\n");
				stream_msg=(stream_update_msg_t *)dummy;
				debug_printf("Guid: %s, T: %lld, S: %s, V: %f\n",
						stream_msg->guid,
						stream_msg->timestamp,
						stream_msg->stream,
						stream_msg->value);
				sqlite3_bind_text(stmt,1,stream_msg->guid,strlen(stream_msg->guid),SQLITE_STATIC);
				sqlite3_bind_text(stmt,2,stream_msg->stream,strlen(stream_msg->stream),SQLITE_STATIC);
				sqlite3_bind_int64(stmt,3,stream_msg->timestamp);
				sqlite3_bind_double(stmt,4,stream_msg->value);
				sqlite3_bind_int(stmt,5,0);

				ts=getTimestampMs(NULL);
        lock_count=0;
				do{
					retval=sqlite3_step(stmt);
					if(retval!=SQLITE_DONE){
            if(retval==SQLITE_BUSY){
              if((++lock_count)%10000 == 0) {
                print_error("Database locked, tried %d times\n",lock_count);
              }
            }else{
  						print_fatal("Could not insert data: %d\n",retval);
            }
            usleep(1341);
					}
				}while(retval!=SQLITE_DONE);
				sqlite3_reset(stmt);
				te=getTimestampMs(NULL);

				debug_printf("Insert time: %lldms\n",te-ts);

				break;
		}
		now=getTimestampMs(NULL);
		if( now > last_clean + 60*60*1e3) {
			db_cleanup(db);
			last_clean=now;
		}
	}

	printf("Closing database\n");
	sqlite3_close(db);

}
