#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <curl/curl.h>
#include <json-c/json.h>

#include "debug.h"
#include "devicehive.h"
#include "data_logger.h"

int do_run;
int debug;
static	CURL *curl;

typedef struct known_device{
  char guid[512];
  struct known_device *next;
} known_device_t;

known_device_t *known_devices=NULL;


void signal_handler(int signo) {
	printf("Signal detected\n");
	do_run=0;
}

size_t write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
     return size * nmemb;
}

long post_data_priv(const char *path, char *data, char *method){
	CURLcode res;
	long http_code = 0;
	char url[2048];

	sprintf(url,"https://hive.ae101.net/%s",path);
	debug_printf("Sending '%s' to '%s'\n",data,url);
  print_info("Posting to: %s\n",url);

  
	/* get a curl handle */ 
	if(curl) {
    curl_easy_reset(curl);
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
  debug_printf("curl_easy_perform\n");
		res = curl_easy_perform(curl);
		/* Check for errors */ 
		if(res != CURLE_OK)
			fprintf(stderr, "curl_easy_perform() failed: %s\n",
					curl_easy_strerror(res));

		curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
		debug_printf("Curl res: %d\n",res);
		debug_printf("Curl Status code: %ld\n",http_code);
		/* always cleanup */ 
	}

  return http_code;
}

long post_data(const char *path, const char *data){
	return post_data_priv(path,data,NULL);
}

long put_data(const char *path, const char *data){
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

int check_known_devices(char *guid) {

  int found=0;
  known_device_t *cur = known_devices;

  debug_printf("Searching for device: %s\n",guid);
  while(cur!=NULL){
    debug_printf("Cur: %s\n",cur->guid);
    if(strcmp(cur->guid,guid)==0){
      found=1;
    }

    cur=cur->next;
  }

  if(!found) {
    //Find last entry
    if(known_devices==NULL){
      //First entry
      known_devices=(known_device_t *)calloc(sizeof(known_device_t),1);
      debug_printf("Creating first device: 0x%08x\n",known_devices);
      cur=known_devices;
    }else{
      //Find last
      cur=known_devices;
      while(cur->next!=NULL){
        cur=cur->next;
      }

      cur->next=(known_device_t *)calloc(sizeof(known_device_t),1);
      cur=cur->next;
    }

    debug_printf("Inserting device: 0x%08x\n",cur);
    sprintf(cur->guid,"%s",guid);

    dh_device_register(cur->guid);
  }
  
  return found;
}

int send_double_callback(void *handle, int argc, char **argv, char **col_name){
	char timebuf[100];
	char guid[1024];
	char stream[1024];
	long long timestamp;
	double value;
  long long id;
	json_object *devices = *((json_object **)handle);
  json_object *device;
  json_object *ids;
  json_object *batch;
  json_object *batch_parameters;
  json_object *notifications;
  json_object *notification;
  json_object *parameters;

//	debug_printf("Double callback: %d\n",argc);
	if(argc==6){
		id=strtoll(argv[0],NULL,10);
//		debug_printf("Id: %lld\n",id);
		sprintf(guid,"%s",argv[1]);
//		debug_printf("GUID: %s\n",guid);
		sprintf(stream,"%s",argv[2]);
//		debug_printf("Stream: %s\n",stream);
		timestamp=strtoll(argv[3],NULL,10);
//		debug_printf("Timestamp: %lld\n",timestamp);
		value=atof(argv[4]);
//		debug_printf("Value: %f\n",value);
		getRFC3339(timestamp,timebuf);
//		debug_printf("Timebuf: %s\n",timebuf);

    if(json_object_object_get_ex(devices, guid, &device) == 0) {
      device=json_object_new_object();
      json_object_object_add(devices,guid,device);

      //Holds the ids from the database, that must have is_logged changed to
      //1 on success
      ids=json_object_new_array();
      json_object_object_add(device,"ids",ids);

      batch=json_object_new_object();
      batch_parameters=json_object_new_object();

      json_object_object_add(device,"batch",batch);

      json_object_object_add(batch,"notification",json_object_new_string("batch"));
      json_object_object_add(batch,"parameters",batch_parameters);
      notifications = json_object_new_array();
      json_object_object_add(batch_parameters,"notifications",notifications);

    }else{
      json_object_object_get_ex(device,"batch",&batch);
      json_object_object_get_ex(batch,"parameters",&batch_parameters);
      json_object_object_get_ex(batch_parameters,"notifications",&notifications);
      json_object_object_get_ex(device,"ids",&ids);
    }

    notification=json_object_new_object();
    json_object_object_add(notification,"notification",json_object_new_string("stream"));
    parameters=json_object_new_object();
    json_object_object_add(parameters,"stream",json_object_new_string(stream));
    json_object_object_add(parameters,"timestamp",json_object_new_string(timebuf));
    json_object_object_add(parameters,"value",json_object_new_double(value));
    json_object_object_add(notification,"parameters",parameters);

    json_object_array_add(ids,json_object_new_int64(id));

    json_object_array_add(notifications,notification);

    
	}else{
		printf("Bad response from sql database: %d\n",argc);
	}

	return 0;

}

int send_string_callback(void *handle, int argc, char **argv, char **col_name){
	char timebuf[100];
	char guid[1024];
	char stream[1024];
	char value[1024];
	long long timestamp;
  long long id;
	json_object *devices = *((json_object **)handle);
  json_object *device;
  json_object *ids;
  json_object *batch;
  json_object *batch_parameters;
  json_object *notifications;
  json_object *notification;
  json_object *parameters;

	debug_printf("String callback: %d\n",argc);
	if(argc==6){
		id=strtoll(argv[0],NULL,10);
		debug_printf("Id: %lld\n",id);
		sprintf(guid,"%s",argv[1]);
		debug_printf("GUID: %s\n",guid);
		sprintf(stream,"%s",argv[2]);
		debug_printf("Stream: %s\n",stream);
		timestamp=strtoll(argv[3],NULL,10);
		debug_printf("Timestamp: %lld\n",timestamp);
		sprintf(value,"%s",argv[4]);
		debug_printf("Value: %s\n",value);
		getRFC3339(timestamp,timebuf);
		debug_printf("Timebuf: %s\n",timebuf);

    if(json_object_object_get_ex(devices, guid, &device) == 0) {
      device=json_object_new_object();
      json_object_object_add(devices,guid,device);

      //Holds the ids from the database, that must have is_logged changed to
      //1 on success
      ids=json_object_new_array();
      json_object_object_add(device,"ids",ids);

      batch=json_object_new_object();
      batch_parameters=json_object_new_object();

      json_object_object_add(device,"batch",batch);

      json_object_object_add(batch,"notification",json_object_new_string("batch"));
      json_object_object_add(batch,"parameters",batch_parameters);
      notifications = json_object_new_array();
      json_object_object_add(batch_parameters,"notifications",notifications);

    }else{
      json_object_object_get_ex(device,"batch",&batch);
      json_object_object_get_ex(batch,"parameters",&batch_parameters);
      json_object_object_get_ex(batch_parameters,"notifications",&notifications);
      json_object_object_get_ex(device,"ids",&ids);
    }

    notification=json_object_new_object();
    json_object_object_add(notification,"notification",json_object_new_string("stream"));
    parameters=json_object_new_object();
    json_object_object_add(parameters,"stream",json_object_new_string(stream));
    json_object_object_add(parameters,"timestamp",json_object_new_string(timebuf));
    json_object_object_add(parameters,"value",json_object_new_string(value));
    json_object_object_add(notification,"parameters",parameters);

    json_object_array_add(ids,json_object_new_int64(id));

    json_object_array_add(notifications,notification);

    
	}else{
		printf("Bad response from sql database: %d\n",argc);
	}

	return 0;

}

int send_strings_to_hive(sqlite3 *db){
  int retval,i;
  long http_code;
  long long id;
  const char *str;
  char *sql_err;
  char url[1024];
	char query[1024];
  json_object *devices;
  json_object *ids;
  json_object *notification;

    devices=json_object_new_object();
  retval=sqlite3_exec(db,"SELECT * FROM stream_string_data WHERE is_logged=0 LIMIT 1000;",send_string_callback, &devices, &sql_err);
  if(retval != SQLITE_OK){
    printf("Select error: %s\n",sql_err);
    sqlite3_free(sql_err);
    return -1;
  }	
  sqlite3_free(sql_err);

  json_object_object_foreach(devices,guid,device){
    debug_printf("Key: %s\n",guid);
    check_known_devices(guid);
    json_object_object_get_ex(device,"batch",&notification);
    json_object_object_get_ex(device,"ids",&ids);
    str = json_object_to_json_string(notification);
    sprintf(url,"device/%s/notification",guid);
    http_code = post_data(url,str);

    if(http_code==200){
      for(i=0;i<json_object_array_length(ids);i++){
        id = json_object_get_int64(json_object_array_get_idx(ids,i));
        sprintf(query,"UPDATE stream_string_data SET is_logged=1 WHERE id = %lld;",id);
        sql_exec(db,query);
      }
    }else{
      print_error("Could not post notifications: %ld\n",http_code);
      return -1;
    }


    debug_printf("Posting %s\n",str);
  }
  json_object_put(devices);
  debug_printf("Getting values for server DONE\n");
    return 0;
}




int main(int argc, char *argv[]){
	int mq,retval,i;
	void *data = calloc(1,MQ_MAX_SIZE);
	char *sql_err;
  const char *str;
  char url[1024];
	char query[1024];
  long http_code;
	long long id;
  json_object *devices;
  json_object *ids;
  json_object *notification;
	sqlite3 *db;
	dummy_msg_t *dummy;
	dh_device_register_msg_t *rd_msg;

  if(argc>1){
    //Get debug flag
    debug=atoi(argv[1]);
  }

  if(debug>0){
    print_info("Printing debug information\n");
  }
	
  signal(SIGINT,signal_handler);
	
	curl_global_init(CURL_GLOBAL_ALL);
	curl = curl_easy_init();

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
            case DH_MSG_DEVICE_REGISTER:
              rd_msg=(dh_device_register_msg_t *)data;
              dh_device_register(rd_msg->guid);
              break;
          }
        }
      }
    }while(retval!=-1);

    
    debug_printf("Getting values for server\n");
    devices=json_object_new_object();
    retval=sqlite3_exec(db,"SELECT * FROM stream_data WHERE is_logged=0 LIMIT 1000;",send_double_callback, &devices, &sql_err);
		if(retval != SQLITE_OK){
			printf("Select error: %s\n",sql_err);
		}	
    sqlite3_free(sql_err);

    json_object_object_foreach(devices,guid,device){
      debug_printf("Key: %s\n",guid);
      check_known_devices(guid);
      json_object_object_get_ex(device,"batch",&notification);
      json_object_object_get_ex(device,"ids",&ids);
      str = json_object_to_json_string(notification);
      sprintf(url,"device/%s/notification",guid);
      http_code = post_data(url,str);

      if(http_code==200){
        for(i=0;i<json_object_array_length(ids);i++){
          id = json_object_get_int64(json_object_array_get_idx(ids,i));
          sprintf(query,"UPDATE stream_data SET is_logged=1 WHERE id = %lld;",id);
          sql_exec(db,query);
        }
      }else{
        print_error("Could not post notifications: %ld\n",http_code);
      }


	  	debug_printf("Posting %s\n",str);
    }
    json_object_put(devices);
    debug_printf("Getting values for server DONE\n");

    send_strings_to_hive(db);

		usleep(100000);
	}
		
  curl_easy_cleanup(curl);
  curl_global_cleanup();
  sqlite3_close(db);
	
}
