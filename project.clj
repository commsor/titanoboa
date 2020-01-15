(defproject io.titanoboa/titanoboa "0.8.3"
  :description "titanoboa.io is fully distributed, highly scalable and fault tolerant workflow orchestration platform"
  :url "http://titanoboa.io"
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
                 [com.cemerick/pomegranate "1.1.0" :exclusions [org.tcrawley/dynapath]]
                 [com.taoensso/nippy "2.14.0"]
                 [me.raynes/fs "1.4.6"]
                 [clojure-watch "0.1.14"]
                 [io.methvin/directory-watcher "0.8.0"]
                 [ring-server "0.4.0"]
                 [ring "1.6.3"]
                 [ring/ring-defaults "0.1.5"]
                 [ring-middleware-format "0.7.2" :exclusions [com.cognitect/transit-clj org.clojure/tools.reader circleci/clj-yaml]]
                 [circleci/clj-yaml "0.5.6" :exclusions [org.flatland/ordered]]
                 [org.flatland/ordered "1.5.7"]
                 [com.cognitect/transit-clj "0.8.319"]
                 [com.taoensso/timbre "4.7.0"]
                 [compojure "1.5.2"]
                 [buddy "2.0.0"]
                 [clj-http "3.9.1"]
                 [org.clojure/tools.reader "1.3.2"]
                 [clj-time "0.14.0"]
                 [org.clojure/java.jdbc "0.7.1"]
                 [com.mchange/c3p0 "0.9.5.2"]
                 [honeysql "0.9.1"]
                 [org.postgresql/postgresql "9.4.1208"]
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

  :repositories [["atlassian" 	"https://maven.atlassian.com/3rdparty/"]]

  :jar-name "titanoboa.jar"
  :target-path "build/%s/"
  :libdir-path "lib"
  :main titanoboa.server ;;titanoboa.system
  :aot [titanoboa.server]
  :clean-targets ^{:protect false} [:target-path]

  :source-paths ["src/cljc" "src/clj" "src/script"]
  :aliases {"package" ["do" "clean," "jar," "libdir"]})
