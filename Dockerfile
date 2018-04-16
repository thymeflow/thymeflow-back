FROM openjdk:8-alpine

MAINTAINER "David Montoya" <david@montoya.one>

ENV SBT_VERSION 0.13.15
ENV SBT_INSTALL_PATH /usr/local
ENV SBT_HOME ${SBT_INSTALL_PATH}/sbt
ENV PATH ${PATH}:${SBT_HOME}/bin

# Install SBT
RUN apk add --update --no-cache \
      curl \
      bash && \
    curl -sL "http://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz" | \
      gunzip | tar -x -C $SBT_INSTALL_PATH

RUN addgroup -g 1000 app && adduser -u 1000 -G app -D -s /bin/false -h /app app

USER app

RUN mkdir /app/src /app/logs /app/data && ln -s /app/logs /app/src/logs && ln -s /app/data /app/src/data

WORKDIR /app/src

COPY --chown=app:app  build.sbt ./build.sbt

RUN sbt update

COPY --chown=app:app . .

RUN sbt compile

EXPOSE 8080

CMD sbt thymeflow/run
