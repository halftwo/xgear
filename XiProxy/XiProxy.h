#ifndef XiProxy_h_
#define XiProxy_h_

extern char xp_the_ip[64];
extern int xp_log_level;
extern int64_t xp_ultra_slow_msec;
extern int64_t xp_slow_warning_msec;
extern unsigned int xp_refresh_time;
extern int xp_delay_msec;


char *xp_get_time_str(time_t t, char *buf);


#endif
