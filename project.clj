(defproject io.titanoboa/titanoboa "1.0.0-alfa.1"
  :description "titanoboa.io is fully distributed, highly scalable and fault tolerant workflow orchestration platform"
  :url "https://titanoboa.io"
  :license {:name "GNU Affero General Public License"
            :url "https://www.gnu.org/licenses/agpl-3.0.en.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [pl.joegreen/lambda-from-string "1.6" :exclusions [org.eclipse.jdt.core.compiler/ecj]]
                 [com.stuartsierra/component "0.3.1"]
                 [org.clojure/core.async "0.4.490"]
                 [org.clojure/tools.logging "0.4.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.clojure/java.classpath "0.3.0"]
                 [org.tcrawley/dynapath "1.0.0"]
                 [org.xeustechnologies/jcl-core "2.8"  :exclusions [org.slf4j/slf4j-api]]
                 [com.cemerick/pomegranate "1.1.0" :exclusions [org.tcrawley/dynapath org.apache.httpcomponents/httpclient]]
                 [com.taoensso/nippy "2.14.0"]
                 [me.raynes/fs "1.4.6"]
                 [clojure-watch "0.1.14"]
                 [io.methvin/directory-watcher "0.8.0"] ;;:exclusions [net.java.dev.jna/jna]
                 [ring-server "0.4.0"]
                 [ring "1.6.3"]
                 [ring/ring-defaults "0.1.5"]
                 [cheshire "5.9.0"]
                 [ring-middleware-format "0.7.4" :exclusions [com.cognitect/transit-clj org.clojure/tools.reader circleci/clj-yaml cheshire]]
                 [circleci/clj-yaml "0.5.6" :exclusions [org.flatland/ordered]]
                 [org.flatland/ordered "1.5.7"]
                 [com.cognitect/transit-clj "0.8.319"]
                 [com.taoensso/timbre "4.10.0"]
                 [compojure "1.5.2"]
                 [buddy "2.0.0"]
                 [clj-http "3.12.3"]
                 [org.clojure/tools.reader "1.3.2"]
                 [clj-time "0.14.0"]
                 [org.clojure/java.jdbc "0.7.1"]
                 [org.mule.mchange/c3p0 "0.9.5.2-MULE-002"]
                 [honeysql "0.9.1"]
                 ;;uncomment following for RDBMS archival use - or just add it into the external dependencies file ;)
                 #_[org.postgresql/postgresql "9.4.1208"]
                 ;;uncomment following for Rabbit MQ use as a job channel - or just add it into the external dependencies file ;)
                 [com.novemberain/langohr "3.5.0" :exclusions [cheshire]]
                 [io.titanoboa/titanoboa-java "0.1.0"]
                 [io.titanoboa/compliment "0.3.9"]]

  :plugins [[lein-libdir "0.1.1"]
            [lein-licenses "0.2.2"]
            [io.titanoboa/lein-ubersource "0.1.2"]]

  :profiles {:java8jre {:dependencies [[pl.joegreen/lambda-from-string "1.6"]
                                       [org.tcrawley/dynapath "0.2.4"]
                                       [com.cemerick/pomegranate "1.0.0" :exclusions [org.tcrawley/dynapath]]]}
             :java8jdk {:dependencies [[org.tcrawley/dynapath "0.2.4"]
                                       [com.cemerick/pomegranate "1.0.0" :exclusions [org.tcrawley/dynapath]]]}}

  :repositories [["atlassian" 	"https://maven.atlassian.com/3rdparty/"] ["mule" "https://repository.mulesoft.org/releases/"]]

  :test-selectors {:default (fn [m] (not (or (:integration m) (:rmq m))))
                   :integration :integration
                   :rmq :rmq}
  :jar-name "titanoboa.jar"
  :target-path "build/%s/"
  :libdir-path "lib"
  :main titanoboa.server ;;titanoboa.system
  :aot [titanoboa.server]
  :clean-targets ^{:protect false} [:target-path :libdir-path]

  :source-paths ["src/cljc" "src/clj" "src/script"]
  :aliases {"package" ["do" "clean," "jar," "libdir"]})
