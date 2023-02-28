FROM openjdk:11 as builder

COPY . /srv

RUN cd /srv && ./gradlew install

FROM openjdk:11-jre

COPY --from=builder /srv/app/build/install/app /opt/bench

ENV PATH /opt/bench/bin:$PATH
