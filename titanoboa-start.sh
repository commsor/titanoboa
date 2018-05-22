#!/bin/bash 
nohup sudo java -Dboa.server.config.path=/mnt/efs/4mevideo/titanoboa/config/boa_server_config_local-on-aws.clj -Dboa.server.dependencies.path=/mnt/efs/4mevideo/titanoboa/config/ext-dependencies.clj -jar titanoboa.jar &
