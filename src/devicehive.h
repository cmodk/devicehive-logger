typedef struct {
	long mtype;
	char guid[256];
}dh_device_register_msg_t;

int dh_send_double(char *guid, char *stream, long long timestamp, double value);

int dh_mq_init();
int dhmq_device_register(int mq, char *guid);
