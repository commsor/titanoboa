(defproject titanoboa "0.1.0-SNAPSHOT"
  :description "titanoboa.io is fully distributed, highly scalable and fault tolerant FaaS and workflow orchestration platform"
  :url "http://titanoboa.io"
  :license {:name "GNU Affero General Public License"
            :url "https://www.gnu.org/licenses/agpl-3.0.en.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [pl.joegreen/lambda-from-string "1.6"] ;;Note: if running on JDK use this instead:  [pl.joegreen/lambda-from-string "1.6" :exclusions [org.eclipse.jdt.core.compiler/ecj]]
                 [com.stuartsierra/component "0.3.1"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.logging "0.3.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.clojure/java.classpath "0.2.3"]
                 [org.tcrawley/dynapath "0.2.4"];;"0.2.4"
                 [org.xeustechnologies/jcl-core "2.8"  :exclusions [org.slf4j/slf4j-api]]
                 [com.cemerick/pomegranate "1.0.0" :exclusions [org.tcrawley/dynapath]]
                 [com.taoensso/nippy "2.12.1"]
                 [me.raynes/fs "1.4.6"]
                 [clojure-watch "0.1.14"]
                 [ring-server "0.4.0"]
                 [ring "1.6.3"]
                 [ring/ring-defaults "0.1.5"]
                 [ring-middleware-format "0.7.0" :exclusions [com.cognitect/transit-clj]]
                 [com.cognitect/transit-clj "0.8.290"]
                 [com.taoensso/timbre "4.7.0"]
                 [compojure "1.5.2"]
                 [buddy "2.0.0"]
                 [clj-http "3.4.1"]
                 [prone "0.8.2"]
                 [org.clojure/tools.reader "1.0.0"]
                 [eval-soup "1.2.2"]
                 [frankiesardo/linked "1.2.9"]
                 [fipp "0.6.10"]
                 [secretary "1.2.3"]
                 [clj-time "0.14.0"]
                 [org.clojure/java.jdbc "0.7.1"]
                 [com.mchange/c3p0 "0.9.5.2"]
                 [honeysql "0.9.1"]
                 [postgresql/postgresql "9.4.1208-jdbc42-atlassian-hosted"]]

  :repositories [["atlassian" 	"https://maven.atlassian.com/3rdparty/"]]

  :uberjar-name "titanoboa.jar"
  :main titanoboa.server ;;titanoboa.system
  :aot [titanoboa.server]
  :clean-targets ^{:protect false} [:target-path]

  :source-paths ["src/cljc" "src/clj" "src/script"])
