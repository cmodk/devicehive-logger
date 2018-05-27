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
int debug;

void signal_handler(int signo) {
	printf("Signal detected\n");
	do_run=0;
}

void db_cleanup_table(sqlite3 *db, char *table){
  char query[1024];
  sprintf(query,"DELETE FROM %s WHERE is_logged=1;",table);
  print_info("Cleaning table: %s\n",table);
	sql_exec(db,query);

  print_info("Vacuuming database\n");
  //Vacuum the database
  if(sql_exec(db,(char *)"VACUUM;")==SQLITE_CORRUPT){
    char cmd[1024];
    sprintf(cmd,"mv /var/lib/data_logger.db /var/lib/data_logger_old.db");
    print_fatal("Database corruptness, creating new\n");
  }
  print_info("Cleaning completed\n");

}

void db_cleanup(sqlite3 *db) {
  db_cleanup_table(db,"stream_data");
  db_cleanup_table(db,"stream_string_data");
}

int main(int argc, char *argv[]) {
	int mq,retval;
	void *data = calloc(1,MQ_MAX_SIZE);
	char query[1024];
	long long ts,te;
	dummy_msg_t *dummy;
	stream_update_msg_t *stream_msg;
	stream_string_update_msg_t *stream_string_msg;
	sqlite3 *db;
	sqlite3_stmt *stmt;
	sqlite3_stmt *stmt_string;
	long long last_clean=0;
	long long now;
  struct msqid_ds mq_info;
  int lock_count;

  sprintf(query,"echo %d > /proc/sys/kernel/msgmax",10*1024*1024);
	system(query);
  sprintf(query,"echo %d > /proc/sys/kernel/msgmnb",10*1024*1024);
	system(query);

	if((mq=data_logger_mq_init()) == -1){
		return -1;
	}

  if(msgctl(mq, IPC_STAT, &mq_info)!=0){
    perror("Could not get status for message queue");
    return -1;
  }
  
  if(mq_info.msg_qbytes<512*1024*5) {
    printf("Maximum bytes in queue: %d\n",mq_info.msg_qbytes);
    mq_info.msg_qbytes=1024*1024*5;

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

  sprintf(query,"INSERT INTO stream_string_data VALUES(NULL,?1,?2,?3,?4,?5);");
	if(sqlite3_prepare_v2(db,query,-1,&stmt_string,0)){
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

      case DATALOGGER_MSG_STREAM_STRING_UPDATE:
        sprintf(query,"INSERT INTO string_stream_data VALUES(NULL,?1,?2,?3,?4,?5);");
        print_info("Got stream string: update\n");
				stream_string_msg=(stream_string_update_msg_t *)dummy;
				print_info("Guid: %s, T: %lld, S: %s, V: %s\n",
						stream_string_msg->guid,
						stream_string_msg->timestamp,
						stream_string_msg->stream,
						stream_string_msg->value);
				sqlite3_bind_text(stmt_string,1,stream_string_msg->guid,strlen(stream_string_msg->guid),SQLITE_STATIC);
				sqlite3_bind_text(stmt_string,2,stream_string_msg->stream,strlen(stream_string_msg->stream),SQLITE_STATIC);
				sqlite3_bind_int64(stmt_string,3,stream_string_msg->timestamp);
				sqlite3_bind_text(stmt_string,4,stream_string_msg->value,strlen(stream_string_msg->value),SQLITE_STATIC);
				sqlite3_bind_int(stmt_string,5,0);

				ts=getTimestampMs(NULL);
        lock_count=0;
				do{
					retval=sqlite3_step(stmt_string);
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
				sqlite3_reset(stmt_string);
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
