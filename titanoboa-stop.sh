#!/bin/bash 
kill -SIGINT $(ps aux | grep titanoboa.jar | grep sudo | awk '{ print $2 }')
