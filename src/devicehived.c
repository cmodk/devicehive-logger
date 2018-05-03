#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <curl/curl.h>

#include "debug.h"
#include "devicehive.h"
#include "data_logger.h"

int do_run;

void signal_handler(int signo) {
	printf("Signal detected\n");
	do_run=0;
}

size_t write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
     return size * nmemb;
}

int post_data_priv(char *path, char *data, char *method){
	CURL *curl;
	CURLcode res;
	long http_code = 0;
	char url[2048];

	sprintf(url,"https://hive.ae101.net/%s",path);
	debug_printf("Sending '%s' to '%s'\n",data,url);

	/* get a curl handle */ 
	curl = curl_easy_init();
	if(curl) {
		//curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		/* First set the URL that is about to receive our POST. This URL can
		 *        just as well be a https:// URL if that is what should receive the
		 *               data. */ 
		curl_easy_setopt(curl, CURLOPT_URL, url);
		/* Now specify the POST data */ 
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);

		struct curl_slist *headers = NULL;
		headers = curl_slist_append(headers, "Authorization: Bearer TestToken");
		headers = curl_slist_append(headers, "Content-Type: application/json");
		res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER,headers);

		if(method!=NULL){
			curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method); /* !!! */
		}

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);

		/* Perform the request, res will get the return code */ 
		res = curl_easy_perform(curl);
		/* Check for errors */ 
		if(res != CURLE_OK)
			fprintf(stderr, "curl_easy_perform() failed: %s\n",
					curl_easy_strerror(res));

		curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
		debug_printf("Curl res: %d\n",res);
		debug_printf("Curl Status code: %ld\n",http_code);
		/* always cleanup */ 
		curl_easy_cleanup(curl);
	}
}

int post_data(char *path, char *data){
	return post_data_priv(path,data,NULL);
}

int put_data(char *path, char *data){
	return post_data_priv(path,data,"PUT");
}

int dh_device_register(char *guid) {
	char data[2048];
	char path[1024];

	sprintf(data,"{\"name\":\"%s\",\"key\":\"TestToken\",\"device_type_id\":1}",guid);
	sprintf(path,"device/%s",guid);

	put_data(path,data);

	return 0;
}

int send_double_callback(void *handle, int argc, char **argv, char **col_name){
	char timebuf[100];
	char data[2048];
	char guid[1024];
	char stream[1024];
	long long id;
	long long timestamp;
	double value;
	char query[1024];
	char url[1024];
	sqlite3 *db = (sqlite3 *)handle;

	debug_printf("Double callback: %d\n",argc);
	if(argc==6){
		id=strtoll(argv[0],NULL,10);
		debug_printf("Id: %lld\n",id);
		sprintf(guid,"%s",argv[1]);
		debug_printf("GUID: %s\n",guid);
		sprintf(stream,"%s",argv[2]);
		debug_printf("Stream: %s\n",stream);
		timestamp=strtoll(argv[3],NULL,10);
		debug_printf("Timestamp: %lld\n",timestamp);
		value=atof(argv[4]);
		debug_printf("Value: %f\n",value);
		getRFC3339(timestamp,timebuf);
		debug_printf("Timebuf: %s\n",timebuf);

		sprintf(data,"{\"notification\":\"stream\",\"parameters\":{\"stream\":\"%s\",\"value\":%f,\"timestamp\":\"%s\"}}",stream,value,timebuf);
		debug_printf("Posting %s\n",data);
		
		sprintf(url,"device/%s/notification",guid);
		post_data(url,data);

		sprintf(query,"UPDATE stream_data SET is_logged=1 WHERE id = %lld;",id);
		sql_exec(db,query);
	}else{
		printf("Bad response from sql database: %d\n",argc);
	}

	return 0;

}

int main(int argc, char *argv[]){
	int mq,retval;
	void *data = calloc(1,MQ_MAX_SIZE);
	char *sql_err;
	sqlite3 *db;
	dummy_msg_t *dummy;
	dh_device_register_msg_t *rd_msg;
	
	curl_global_init(CURL_GLOBAL_ALL);

	mq=dh_mq_init();
	if(mq==-1){
		perror("Could not open message queue");
		return -1;
	}

	if((db=sql_open())==NULL){
		print_fatal("Could not open database\n");
		return -1;
	}

	do_run=1;
	while(do_run){
		
    //Check for commands
    do{
      memset(data,0,MQ_MAX_SIZE);
      retval=msgrcv(mq,data,MQ_MAX_SIZE-sizeof(long),0,IPC_NOWAIT);
      if(retval==-1){
        if(errno!=ENOMSG) {
          perror("MQ error");
        }
      }else{
        if(retval>0){
          dummy=(dummy_msg_t *)data;
          switch(dummy->mtype){
            case 1:
              rd_msg=(dh_device_register_msg_t *)data;
              dh_device_register(rd_msg->guid);
              break;
          }
        }
      }
    }while(retval!=-1);

    retval=sqlite3_exec(db,"SELECT * FROM stream_data WHERE is_logged=0 LIMIT 5;",send_double_callback, db, &sql_err);
		if(retval != SQLITE_OK){
			printf("Select error: %s\n",sql_err);
			sqlite3_free(sql_err);
		}
		usleep(100000);
	}
	
}
