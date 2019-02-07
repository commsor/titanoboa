[![Build Status](https://travis-ci.com/mikub/titanoboa.svg?branch=master)](https://travis-ci.com/mikub/titanoboa)

## Synopsis
Titanoboa is fully distributed, highly scalable and fault tolerant workflow orchestration platform.
It employs hybrid iPaaS concepts and runs on the JVM.
You can run it on-premises or in a cloud.

![alt Logo](https://s3.eu-central-1.amazonaws.com/www.titanoboa.io/tb-logo-dark-nosubtitle.svg)

See also [titanoboa.io](https://titanoboa.io) and our [wiki](https://github.com/mikub/titanoboa/wiki). Predefined workflow steps are [here](https://github.com/mikub/titanoboa-tasklets).

<img src="http://www.titanoboa.io/sample-graph.gif" width="500"/>

## Motivation
Titanoboa has been created with aim to create a workflow platform, that would support all the usual features of complex workflow engines and majority of enterprise integration patterns including:
* sequential and/or **parallel** step execution
* configurable step **retry** upon error and advanced customizable error handling
* advanced branching and conditional flow
* potentially **cyclic** workflow graphs
* **splitter** and **aggregator** (aka map/reduce) patterns which allow for processing of larger data sets
* fully **transactional** nature that assures that all steps are executed even in case of a failover
* full **extensibility** and ability to rapidly develop and deploy new workflows during runtime

In addition to this titanoboa also strives to honor **immutability and functional programming principles**. This enables it to be fully **distributed** and to be **highly available** with no central master server or database. This also helps lift performance limitations and thus titanoboa can be suitable for not only batch processing, but also for performance critical workflows.

Titanoboa's [**GUI**](https://github.com/mikub/titanoboa/wiki/Getting-Started-with-GUI) can be used not only to monitor workflow jobs and server configuration, but provides an in-build IDE with workflow visualisation, properties editor and a repl so as users can rapidly test-develop new workflows directly in there.

<img width="42" height="42" src="https://github.com/mikub/titanoboa/blob/master/doc/clojure.svg"><img width="54" height="54" src="https://github.com/mikub/titanoboa/blob/master/doc/java.svg"><br/>
Titanoboa is **designed for both java & clojure developers** and we are striving to make it **usable even for java developers with no prior clojure knowledge**.

## Predefined Steps
Though it is easy to rapidly develop new workflow steps directly from GUI, there is a number of ready-made steps and tasklets in [this repository](https://github.com/mikub/titanoboa-tasklets).
Some of the steps included are: 
- AWS (EC2, S3, SNS, SQS, SES)
- Http Client
- Smtp Client
- Sftp
- PDF Generation 

and more.

## Prerequisites
Java 8 or higher.

## Installation 
We suggest giving titanoboa's GUI a try as well since it is the best starting point!  
Download titanoboa's distribution including its GUI from https://www.titanoboa.io/distributions/titanoboa-0.7.4-SNAPSHOT_gui_non-commercial_use_only.zip 

    curl --remote-name https://www.titanoboa.io/distributions/titanoboa-0.7.4-SNAPSHOT_gui_non-commercial_use_only.zip

**Note**: _The GUI is free for non-commercial use only_

__Note__: _If you are intending on running titanoboa server on java 8 JRE, download a distribution for JRE instead:_

    curl --remote-name https://www.titanoboa.io/distributions/titanoboa-0.7.4-SNAPSHOT_jre_gui_non-commercial_use_only.zip

Unzip the file:
    
    unzip titanoboa-0.7.4-SNAPSHOT_gui_non-commercial_use_only.zip

then execute the start script:
    
     ./start

In your console you should see bunch of log messages and ultimately you will see
     
     INFO [main] - Started @2338ms

which means the server started successfully. By default both the server and the GUI will start on port 3000 and you can open http://localhost:3000 in your browser.

Congratulations! You have just started your titanoboa server!

You can go ahead and try to create a [sample workflow](https://github.com/mikub/titanoboa/wiki/Getting-Started-with-GUI).

## Installing server without GUI
Download the latest release from https://www.titanoboa.io/distributions/titanoboa-0.7.4-SNAPSHOT.zip .

    curl --remote-name https://www.titanoboa.io/distributions/titanoboa-0.7.4-SNAPSHOT.zip

__Note__: _If you are intending on running titanoboa server on java 8 JRE, download a distribution for JRE instead:_

    curl --remote-name https://www.titanoboa.io/distributions/titanoboa-0.7.4-SNAPSHOT_jre.zip
    
And then follow the instructions above. By default the server will start on port 3000.
     
### Building from the repository
Titanoboa uses leiningen for dependency management, so if you don't have it download it from https://leiningen.org/ and follow its installation instructions.

Clone this repository

    git clone https://github.com/mikub/titanoboa
    
run lein package:

    lein package
    
in case that your target platform is Java 8 JRE use following profile:

    lein with-profile java8jre package

this will generate titanoboa jar file in the _build_ directory and will also download all libraries into _lib_ folder.

If you want to use GUI you can clone the titanoboa-gui repository as well:

    git clone https://github.com/mikub/titanoboa-gui
    
**Note**: _The GUI is free for non-commercial use only_

then merge GUI's _public_ folder into the uberjar:

    zip -r -g ./build/titanoboa.jar public

then run the start script:
    
     ./start

In your console you should see bunch of log messages and ultimately you will see
     
     INFO [main] - Started @3238ms

which means the server started successfully. By default the server will start on port 3000.

Congratulations! You have just built & started your titanoboa server!

If you included GUI you can go ahead and try to create a [sample workflow](https://github.com/mikub/titanoboa/wiki/Getting-Started-with-GUI).

### Server Configuration
Server configuration and external dependencies file can be specified by system properties `boa.server.config.path` and `boa.server.dependencies.path`:

     java -Dboa.server.config.path=boa_server_config_local.clj -Dboa.server.dependencies.path=ext-dependencies.clj -cp "./build/titanoboa.jar:./lib/*" titanoboa.server
     
See [Server configuration wiki](https://github.com/mikub/titanoboa/wiki/Server-Configuration) for more details.


## Getting Started
Before you start, it might be a good idea to get familiar with titanoboa's [concepts](https://github.com/mikub/titanoboa/wiki) & [workflow design principles](https://github.com/mikub/titanoboa/wiki/Designing-Workflows).

### Develop & Test Workflows with titanoboa GUI
Titanoboa GUI is a good place to start devloping and testing workflows:

[![]( https://github.com/mikub/titanoboa/blob/master/doc/generate-report17-change-details.png )](https://raw.githubusercontent.com/mikub/titanoboa/master/doc/generate-report17-change-details.png )

[![]( https://github.com/mikub/titanoboa/blob/master/doc/generate-report19-jobs.png )](https://raw.githubusercontent.com/mikub/titanoboa/master/doc/generate-report19-jobs.png )

See an example in our wiki on how to create a [sample workflow](https://github.com/mikub/titanoboa/wiki/Getting-Started-with-GUI).

### <img width="42" height="42" src="https://github.com/mikub/titanoboa/blob/master/doc/clojure.svg"> Develop & Test Workflows Locally in Your Clojure REPL
If you cannot use GUI and do not want to use REST API, you can as well just start REPL locally and play with titanoboa there.
Either build titanoboa from repo or get it as _leiningen_ or _maven_ dependency:

[![Clojars Project](https://img.shields.io/clojars/v/io.titanoboa/titanoboa.svg)](https://clojars.org/io.titanoboa/titanoboa)

```clojure
[io.titanoboa/titanoboa "0.7.3"]
```

```xml
<dependency>
  <groupId>io.titanoboa</groupId>
  <artifactId>titanoboa</artifactId>
  <version>0.7.3</version>
</dependency>
```
then fire off a repl and start:

#### Define a sample "Hello World!" workflow:
```clojure
(ns local-system-test
  (:require [titanoboa.system]
            [titanoboa.processor]))

(defn hello-fn [p]
  {:message (str "Hello " (or (:name p) "human") "!")
   :return-code (nil? (:name p))})

(defn greet-fn [p]
  {:message (str (:message p) " Nice to meet you!")})

(defn fill-in-blanks [p]
  {:message (str (:message p) " What is your name?")})
  
(def job-def {:first-step "step1"
              :name       "test"
              :properties {:name nil}
              :steps      [{:id          "step1"
                            :type :custom
                            :supertype :tasklet
                            :next [[false "step2"] [true "step3"]]
                            :workload-fn 'local-system-test/hello-fn
                            :properties  {}}
                           {:id          "step2" :type :custom :supertype :tasklet
                            :workload-fn 'local-system-test/greet-fn
                            :next        []
                            :properties  {}}
                           {:id          "step3" :type :custom :supertype :tasklet
                            :workload-fn 'local-system-test/fill-in-blanks
                            :next        []
                            :properties  {}}]})
```

#### Start a simple local system with workers that will process the job:
```clojure
(def new-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(def jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(def finished-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))

(titanoboa.system/start-system! :core-local
                                  {:core-local {:system-def   #'titanoboa.system.local/local-core-system
                                          :worker-def   #'titanoboa.system.local/local-worker-system
                                          :worker-count 2}}
                                  {:new-jobs-chan      new-jobs-chan
                                   :jobs-chan          jobs-chan
                                   :finished-jobs-chan finished-jobs-chan
                                   :node-id            "localhost"
                                   :eviction-interval  (* 1000 60 5)
                                   :eviction-age       (* 1000 60 10)
                                   :jobs-repo-path     "repo-test/"
                                   :job-folder-path    "job-folders/"})
                                   
 INFO [nREPL-worker-0] - Starting system :core-local ...
 INFO [nREPL-worker-0] - Starting CacheEvictionComponent...
 INFO [CacheEvictionComponent thread 0] - Starting CacheEvictionComponent thread [ CacheEvictionComponent thread 0 ].
 INFO [nREPL-worker-0] - Starting action processor pool...
 INFO [nREPL-worker-0] - Starting to watch repo folder for changes:  dev-resources/repo-test/
 INFO [nREPL-worker-0] - System :core-local started
=> true
 
(titanoboa.system/start-workers! :core-local
                                   {:core-local {:system-def   #'titanoboa.system.local/local-core-system
                                           :worker-def   #'titanoboa.system.local/local-worker-system
                                           :worker-count 2}})
                                           
 INFO [nREPL-worker-1] - Starting 2 workers for system :core-local :
 INFO [nREPL-worker-1] - Starting a worker for system :core-local ...
 INFO [nREPL-worker-1] - Starting job worker....
 INFO [nREPL-worker-1] - Starting a worker for system :core-local ...
 INFO [nREPL-worker-1] - Starting job worker....
=> nil                                           
```
#### Start the job:
```clojure
(titanoboa.processor/run-job! :core-local
                              {:jobdef job-def
                               :properties {:name "World"}}
                              true)

 INFO [nREPL-worker-2] - Submitting new job [] into new jobs channel...
 INFO [async-thread-macro-2] - Initializing a new job; First step will be: [ step1 ]
 INFO [async-thread-macro-2] - Retrieved job [ d673c759-4fc6-4af1-bdad-d1dfd0f50f22 ] from jobs channel; Starting step [ step1 ]
 INFO [async-thread-macro-2] - Next step is  step2 ; Submitting into jobs channel for next step's processing...
 INFO [async-thread-macro-1] - Initializing a next step; next step [ step2 ] was found among steps as step [ step2 ]
 INFO [async-thread-macro-2] - Acking main message for step  step1  with thread stack  nil
 INFO [async-thread-macro-1] - Retrieved job [ d673c759-4fc6-4af1-bdad-d1dfd0f50f22 ] from jobs channel; Starting step [ step2 ]
 INFO [async-thread-macro-1] - Looping through finalize-job! fn with thread-stack:  []
 INFO [async-thread-macro-1] - Acking main message for step  step2  with thread stack  nil
 INFO [async-thread-macro-1] - Job  d673c759-4fc6-4af1-bdad-d1dfd0f50f22  has finshed.
=>
{:properties {:name "World", :message "Hello World! Nice to meet you!"},
 :step-start #inst"2018-11-23T07:35:34.218-00:00",
 :step-retries {},
 :tracking-id nil,
  :step-state :completed,
 :start #inst"2018-11-23T07:35:34.203-00:00",
 :history [{:step-state :completed,
            :start #inst"2018-11-23T07:35:34.210-00:00",
            :duration 6,
            :result false,
            :node-id "localhost",
            :id "step1",
            :next-step "step2",
            :exception nil,
            :end #inst"2018-11-23T07:35:34.216-00:00",
            :retry-count nil,
            :thread-stack nil,
            :message "Step [step1] finshed with result [false]\n"}
           {:id "step2",
            :step-state :running,
            :start #inst"2018-11-23T07:35:34.218-00:00",
            :node-id "localhost",
            :retry-count 0}
           {:step-state :completed,
            :start #inst"2018-11-23T07:35:34.218-00:00",
            :duration 1,
            :result nil,
            :node-id "localhost",
            :id "step2",
            :next-step nil,
            :exception nil,
            :end #inst"2018-11-23T07:35:34.219-00:00",
            :retry-count nil,
            :thread-stack nil,
            :message "Step [step2] finshed with result []\n"}],
 :duration 16,
 :state :finished,
 :jobid "d673c759-4fc6-4af1-bdad-d1dfd0f50f22",
 :create-folder? true,
 :node-id "localhost",
 :next-step nil,
 :end #inst"2018-11-23T07:35:34.219-00:00"}
```

When you are done testing you may want to stop the system:
```clojure
(titanoboa.system/stop-all-systems!)

 INFO [nREPL-worker-3] - Stopping all workers for system :core-local
 INFO [nREPL-worker-3] - Stopping job worker gracefully; sending a stop signal to the worker via service bus....
 INFO [nREPL-worker-3] - Stopping job worker gracefully; sending a stop signal to the worker via service bus....
 INFO [nREPL-worker-3] - Stopping system :core-local ...
 INFO [nREPL-worker-3] - Stopping action processor pool...
 INFO [nREPL-worker-3] - Stopping CacheEvictionComponent thread [ CacheEvictionComponent thread 0 ]...
 ```
 ### <img width="48" height="48" src="https://github.com/mikub/titanoboa/blob/master/doc/java.svg"> Developing custom workflow steps in Java 
 Titanoboa is also meant to be used by java developers who (apart from few concepts like [EDN](https://github.com/edn-format/edn)) do not need to be familiar with clojure. If you do not want to use clojure [java interop](https://clojure.org/reference/java_interop) to instantiate your objects and/or invoke your methods, you also have other options:
 
 To create a custom workflow step, simply add a (maven) dependency on [![Clojars Project](https://img.shields.io/clojars/v/io.titanoboa/titanoboa-java.svg)](https://clojars.org/io.titanoboa/titanoboa-java) to your project.
 and create a class that will implement [io.titanoboa.java.IWorkloadFn](https://github.com/mikub/titanoboa-java/blob/master/src/main/java/io/titanoboa/java/IWorkloadFn.java) interface:
 ```java
 public interface IWorkloadFn {
    public Object invoke (Map properties);
}
 ```
 If you then add your project (or the corresponding maven artifact) to titanoboa's [external dependencies](https://github.com/mikub/titanoboa/wiki/Server-Configuration#external-dependencies), you can use your class name in the workflow-fn field. The class will be automatically instantiated as a singleton bean (so it has to have a constructor with no argumet) and all subsequent references to it from any workflow-fn will invoke its __invoke__ method:
 
 ```clojure
 :workload-fn io.titanoboa.java.SampleWorkloadImpl
 ```
or

 ```clojure
 :workload-fn 'io.titanoboa.java.SampleWorkloadImpl
 ```
 or in GUI:
[![]( https://github.com/mikub/titanoboa/blob/master/doc/java-workload.png )](https://raw.githubusercontent.com/mikub/titanoboa/master/doc/java-workload.png )

### Java lambda support
To rapidly development and test new steps, you can also type a lambda function in the GUI and have titanoboa evaluate it during runtime:
[![]( https://github.com/mikub/titanoboa/blob/master/doc/java-lambda-workload.png )](https://raw.githubusercontent.com/mikub/titanoboa/master/doc/java-lambda-workload.png )

## License
Copyright © Miroslav Kubicek

Titanoboa is dual-licensed under either AGPL license or a Commercial license.
