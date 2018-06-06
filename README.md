## Synopsis
Titanoboa is fully distributed, highly scalable and fault tolerant workflow orchestration platform.
It employs hybrid iPaaS and FaaS concepts and runs on the JVM.
You can run it on-premises or in a cloud.

See also http://www.titanoboa.io

## Getting Started
Download the latest release from http://www.titanoboa.io. The release is a jar file.
Download titanoboa GUI titanoboa.io. The GUI is distributed in a form of a zip file. 

    curl --remote-name http://www.titanoboa.io/titanoboa.jar
    curl --remote-name http://www.titanoboa.io/titanoboa-gui.zip
     
__Note__: _If you are intending on runnint titanoboa server on java JDK instead of JRE, download a distribution for JDK instead:_

    curl --remote-name http://www.titanoboa.io/titanoboa4jdk.jar

Unzip the GUI file. It will create a folder named "public".
Then merge the folder into the jar file:
    
    unzip titanoboa-gui.zip
    zip -r -g titanoboa.jar public

then execute the jar:
    
     java -jar titanoboa.jar

In your console you should see bunch of log messages and ultimately you will see
     
     INFO [main] - Started @2338ms

which means the server started successfully. by default the server and the GUI will start on port 3000 so if you are using the GUI you can open http://localhost:3000 in your browser.

Congratulations! You have just started your titanoboa server!

### Prerequisites
Java 8 JRE or JDK. Almost all of the functionality works on Java 7 and higher, however Java Lambda support has been tested only on Java 8.

### Ruinning server without GUI

Titanoboa GUI is great place for developing and designing new workflows as well as for managing their execution and monitoring the status of your server(s).
It is also a great starting point for evaluating and exploring the titanoboa platform.

But since the GUI is free for non-commercial use only, you may want to run titanoboa server only with no GUI. To do so simply skip correspodning steps above and don't download/merge the GUI folder into the titanoboa jar file.

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

## License
Copyright © Miro Kubicek

Distributed under AGPL license.