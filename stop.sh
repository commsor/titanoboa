#!/bin/bash 
kill -SIGINT $(ps aux | grep titanoboa.server | grep sudo | awk '{ print $2 }')
