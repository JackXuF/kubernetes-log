FROM progrium/busybox 
# sign name
MAINTAINER zju

ADD ./Server  /usr/


WORKDIR /usr/

# port 22
EXPOSE 22 

# Tomcat 8080
EXPOSE 8080

ENTRYPOINT ["./Server","-server"]
CMD ["-old"]
