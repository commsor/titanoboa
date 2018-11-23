
## Synopsis
Titanoboa is fully distributed, highly scalable and fault tolerant workflow orchestration platform.
It employs hybrid iPaaS concepts and runs on the JVM.
You can run it on-premises or in a cloud.

![alt Logo](https://s3.eu-central-1.amazonaws.com/www.titanoboa.io/tb-logo-dark-nosubtitle.svg)

See also [titanoboa.io](https://titanoboa.io) and our [wiki](https://github.com/mikub/titanoboa/wiki) 

<img src="http://www.titanoboa.io/sample-graph.gif" width="500"/>

## Motivation
Titanoboa has been created with aim to create a workflow platform, that would support all the usual features of complex workflow engines and majority of enterprise integration patterns including:
* sequential and/or **parallel** step execution
* configurable step **retry** upon error and advanced customizable error handling
* advanced branching and conditional flow
* potentially **cyclic** workflow graphs
* **splitter** and **aggregator** (aka map/reduce) patterns which allows for processing of larger data sets
* fully **transactional** nature that assures that all steps are executed even in case of a failover

In addition to this titanoboa also strives to honor **immutability and functional programming principles**. This enables it to be fully **distributed** and to be **highly available** with no central master server or database. This also helps lift performance limitations and thus titanoboa can be suitable for not only batch processing, but also for performance critical workflows.

Titanoboa's [**GUI**](https://github.com/mikub/titanoboa/wiki/Getting-Started-with-GUI) can be used not only to monitor workflow jobs and server configuration, but provides an in-build IDE with workflow visualisation, properties editor and a repl so as users can rapidly test-develop new workflows directly in there.


## Installation 
Download the latest release from https://www.titanoboa.io/titanoboa.jar. It is a single jar file.

    curl --remote-name https://www.titanoboa.io/titanoboa.jar

__Note__: _If you are intending on running titanoboa server on java JDK instead of JRE, download a distribution for JDK instead:_

    curl --remote-name https://www.titanoboa.io/titanoboa4jdk.jar

then execute the jar:
    
     java -jar titanoboa.jar

In your console you should see bunch of log messages and ultimately you will see
     
     INFO [main] - Started @3238ms

which means the server started successfully. By default the server and will start on port 3000.

Congratulations! You have just started your titanoboa server!

### Running server with GUI

Titanoboa GUI is great place for developing and designing new workflows as well as for managing their execution and monitoring the status of your server(s).
It is also a great starting point for evaluating and exploring the titanoboa platform.

__The GUI is free for non-commercial use only__, so if you just want to explore titanoboa it is the best place to start:

Download the latest release from https://www.titanoboa.io/distributions/gui-non-commercial-use-only/titanoboa.jar. It is a single jar file, the GUI is already in it.

    curl --remote-name https://www.titanoboa.io/distributions/gui-non-commercial-use-only/titanoboa.jar

__Note__: _If you are intending on running titanoboa server on java JDK instead of JRE, download a distribution for JDK instead:_

    curl --remote-name https://www.titanoboa.io/distributions/gui-non-commercial-use-only/titanoboa4jdk.jar

then execute the jar:
    
     java -jar titanoboa.jar

In your console you should see bunch of log messages and ultimately you will see
     
     INFO [main] - Started @3478ms

which means the server started successfully. By default the server and the GUI will start on port 3000 so you can open http://localhost:3000 in your browser to access it.

Now you can go ahead and try to create a [sample workflow](https://github.com/mikub/titanoboa/wiki/Getting-Started-with-GUI).

### Prerequisites
Java 8 JRE or JDK and higher. Almost all of the functionality works on Java 8 and higher, however Java Lambda support has been tested only on Java 8.

### Server Configuration
Server configuration and external dependencies file can be specified by system properties `boa.server.config.path` and `boa.server.dependencies.path`:

     java -Dboa.server.config.path=boa_server_config_local.clj -Dboa.server.dependencies.path=ext-dependencies.clj -jar titanoboa.jar
     
See [Server configuration wiki](https://github.com/mikub/titanoboa/wiki/Server-Configuration) for more details.

### Building from the repository
In case you don't want to download distributed release from our web page but to build it from the repository:
Titanoboa uses leiningen for dependency management, so if you don't have it download it from https://leiningen.org/ and follow its installation instructions.

Clone this repository

    git clone https://github.com/mikub/titanoboa
    
generate uberjar:

    lein uberjar
    
this will generate a big jar file (aka uberjar) in the _target_ directory.

If you want to use GUI you can clone the titanoboa-gui repository as well:

    git clone https://github.com/mikub/titanoboa-gui

then merge GUI's _public_ folder into the uberjar:

    zip -r -g titanoboa.jar public

then execute the jar:
    
     java -jar titanoboa.jar

## Getting Started

### Develop & Test Workflows with titanoboa GUI
Titanoboa GUI is best place to start devloping and testing workflows.

[![]( https://github.com/mikub/titanoboa/blob/master/doc/generate-report17-change-details.png )](https://raw.githubusercontent.com/mikub/titanoboa/master/doc/generate-report17-change-details.png )

[![]( https://github.com/mikub/titanoboa/blob/master/doc/generate-report19-jobs.png )](https://raw.githubusercontent.com/mikub/titanoboa/master/doc/generate-report19-jobs.png )

See an example in our wiki on how to create a [sample workflow](https://github.com/mikub/titanoboa/wiki/Getting-Started-with-GUI).

### Develop & Test Workflows Locally in your REPL or IDE
If you cannot use GUI and do not want to use REST API, you can as well just start REPL locally and play with titanoboa there.
Either build titanoboa from repo or get it as _leiningen_ or _maven_ dependency:

[![Clojars Project](https://img.shields.io/clojars/v/titanoboa.svg)](https://clojars.org/titanoboa)

```clojure
[titanoboa "0.7.1"]
```

```xml
<dependency>
  <groupId>titanoboa</groupId>
  <artifactId>titanoboa</artifactId>
  <version>0.7.1</version>
</dependency>
```

#### Define a sample workflow:
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

## License
Copyright © Miro Kubicek

Titanoboa is dual-licensed under either AGPL license or a Commercial license.
