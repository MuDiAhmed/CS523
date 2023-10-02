### Requirements:
1. Docker 
2. Java
3. Spark
4. HBase
5. HDFS
6. Docker Compose 
7. Maven

### How to run
1. Run support containers `sudo docker compose -f ./docker-compose-supporting-services.yml up`
2. Package modules using maven `java -Dmaven.multiModuleProjectDirectory=<PATH_TO_PROJECT>/Project -Djansi.passthrough=true -Dmaven.home<PATH_TO_MAVEN>/maven3 -Dclassworlds.conf=<PATH_TO_MAVEN>/maven3/bin/m2.conf -Dmaven.ext.class.path=<PATH_TO_MAVEN>/maven/lib/maven-event-listener.jar -Dfile.encoding=UTF-8 -classpath <PATH_TO_CLASSPATHS> package`
3. Run producer jar `spark-submit --class org.cs523.Producer --master <master-url> --deploy-mode cluster <PATH_TO_PRODUCER_JAR>`
4. Run consumer jar `spark-submit --class org.cs523.Receiver --master <master-url> --deploy-mode cluster <PATH_TO_CONSUMER_JAR>`


### Remaining tasks
1. Add analysis (e.g aggregation) part for jobs
2. Change HBase schema
3. Test application using cluster mode