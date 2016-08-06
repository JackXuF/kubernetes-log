FROM progrium/busybox 
# sign name
MAINTAINER zju

ADD ./logstash-forwarder  /root/


WORKDIR /root/

# port 22
EXPOSE 22 

# Tomcat 8080
EXPOSE 8080

ENTRYPOINT ["./logstash-forwarder","-server"]
