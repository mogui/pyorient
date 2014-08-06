#!/bin/bash
cd /usr/local/bin/orientdb/bin
su -c "setuid orientdb ./server.sh" root
