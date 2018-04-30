#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <data_logger.h>

long long getTimestampMs(struct timeval* pTimeval)
{
	struct timeval tv;

	if(pTimeval == NULL)
	{
		gettimeofday(&tv,NULL);
		pTimeval = &tv;
	}

	return ((((long long)pTimeval->tv_sec*1000000)+((long long)pTimeval->tv_usec))/1000);
}

void getRFC3339(long long stamp, char buf[100])
{
	struct tm nowtm;
	time_t nowtime;

	nowtime=stamp/1000;
	gmtime_r(&nowtime,&nowtm);

	char timestr[100];
	strftime(buf,100,"%Y-%m-%dT%H:%M:%S.000000Z",&nowtm);

	char miliStr[100];
	sprintf(miliStr,"%06d",(stamp%1000)*1000);
	memcpy(&(buf[20]),miliStr,6);
	//printf("Timestamp RFC: %s\n",buf);
}

