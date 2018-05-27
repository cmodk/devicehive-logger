#include <unistd.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sqlite3.h>
#include <sys/time.h>

#define MQ_MAX_SIZE 4096

#define DATALOGGER_MSG_STREAM_UPDATE 1
#define DATALOGGER_MSG_STREAM_STRING_UPDATE 2

#define STREAM_TABLE "CREATE TABLE IF NOT EXISTS stream_data("\
	"id INTEGER PRIMARY KEY AUTOINCREMENT,"\
	"guid TEXT,"\
	"stream TEXT,"\
	"timestamp INT,"\
	"value REAL," \
	"is_logged INT);"	

#define STREAM_STRING_TABLE "CREATE TABLE IF NOT EXISTS stream_string_data("\
	"id INTEGER PRIMARY KEY AUTOINCREMENT,"\
	"guid TEXT,"\
	"stream TEXT,"\
	"timestamp INT,"\
	"value TEXT," \
	"is_logged INT);"	


typedef struct {
	long mtype;
	char data[MQ_MAX_SIZE];
} dummy_msg_t;

typedef struct {
	long mtype; 
	char guid[256];
	char stream[256];
	long long timestamp;
	double value;
} stream_update_msg_t;

typedef struct {
	long mtype; 
	char guid[256];
	char stream[256];
	long long timestamp;
  char value[1024];
} stream_string_update_msg_t;


int data_logger_mq_init();

sqlite3 *sql_open(void);
int sql_exec(sqlite3 *db, char *query);
long long getTimestampMs(struct timeval* pTimeval);
void getRFC3339(long long stamp, char buf[100]);

int log_double(int mq, char *guid, char *stream, long long timestamp, double value);

