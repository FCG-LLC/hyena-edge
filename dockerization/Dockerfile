FROM portus.cs.int:5000/prod/cs-base
LABEL maintainer="<devops@collective-sense.com>"
ARG destEnv
# ARG debVer

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install apt-transport-https -y --force-yes

RUN groupadd -g 10033 hyena
RUN adduser --no-create-home --system --shell /bin/false --gecos 'hyena dedicated user' --uid 10033 --gid 10033 --disabled-password hyena

RUN echo "deb http://aptly.cs.int/public xenial $destEnv" >> /etc/apt/sources.list
RUN printf "Package: * \nPin: release a=xenial, o=aptly.cs.int \nPin-Priority: 1600 \n" > /etc/apt/preferences

RUN apt-get update
#    && apt-get install -y hyena=$debVer

COPY *.deb /tmp

RUN (dpkg -i /tmp/*.deb || true) && apt-get install -fy && rm /tmp/*.deb

USER hyena

CMD /usr/bin/hyena
