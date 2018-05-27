#include "data_logger.h"
#include "debug.h"

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
	int i;
	for(i=0; i<argc; i++){
		printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("\n");
	return 0;
}

sqlite3 *sql_open(void){
	sqlite3 *db;
	int retval;

	retval=sqlite3_open("/var/lib/data_logger.db",&db);
	if(retval){
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
		return NULL;
	}

	sql_exec(db,(char *)"PRAGMA synchronous = OFF");
	sql_exec(db,(char *)"PRAGMA journal_mode = MEMORY");
	sql_exec(db,(char *)"PRAGMA encoding = \"UTF-8\"");


  if(sql_exec(db,STREAM_TABLE)) {
    print_fatal("Could not create table for streams\n");
    return NULL;
  }

  if(sql_exec(db,STREAM_STRING_TABLE)) {
    print_fatal("Could not create table for string streams\n");
    return NULL;
  }

  return db;
}

int sql_exec(sqlite3 *db, char *query){
	char *zErrMsg = 0;
	int r;

	debug_printf("Executing: %s\n",query);
	do {
		r = sqlite3_exec(db, query, callback, 0, &zErrMsg);
		if(r){
			print_error("Error executing: %s -> %s\n",query,zErrMsg);
			return -1;
		}
	}while(r==SQLITE_BUSY);

	return 0;
}
