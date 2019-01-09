;;NOTE: this is a sample used for demonstration and testing purposes
{:coordinates [[amazonica "0.3.117" :exclusions [com.amazonaws/aws-java-sdk
                                                 com.amazonaws/amazon-kinesis-client
                                                 com.taoensso/nippy]]
               [com.amazonaws/aws-java-sdk-core "1.11.237"]
               [com.amazonaws/aws-java-sdk-sqs "1.11.237"]
               [com.amazonaws/aws-java-sdk-sns "1.11.237"]
               [com.amazonaws/aws-java-sdk-s3 "1.11.237"]
               [com.amazonaws/aws-java-sdk-ses "1.11.237"]
               [com.amazonaws/aws-java-sdk-ec2 "1.11.237"]
               [clj-pdf "2.2.33"]]
 :require [[amazonica.aws.s3]
           [amazonica.aws.s3transfer]
           [amazonica.aws.ec2]
           [amazonica.aws.simpleemail]
           [clj-pdf.core]]
 :import nil
 :repositories {"central" "https://repo1.maven.org/maven2/"
                "clojars" "https://clojars.org/repo"}}