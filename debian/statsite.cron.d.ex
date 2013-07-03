#
# Regular cron jobs for the statsite package
#
0 4	* * *	root	[ -x /usr/bin/statsite_maintenance ] && /usr/bin/statsite_maintenance
