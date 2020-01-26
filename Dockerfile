FROM golang:1.13

WORKDIR /app

RUN apt-get update && apt-get install -y \
  freeglut3 \
  libgtk2.0-dev \
  libwxgtk3.0-dev \
  libwx-perl \
  libxmu-dev \
  libgl1-mesa-glx \
  libgl1-mesa-dri \
  xdg-utils \
  locales \
  --no-install-recommends \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get autoremove -y \
  && apt-get autoclean

RUN sed -i \
	-e 's/^# \(cs_CZ\.UTF-8.*\)/\1/' \
	-e 's/^# \(de_DE\.UTF-8.*\)/\1/' \
	-e 's/^# \(en_US\.UTF-8.*\)/\1/' \
	-e 's/^# \(es_ES\.UTF-8.*\)/\1/' \
	-e 's/^# \(fr_FR\.UTF-8.*\)/\1/' \
	-e 's/^# \(it_IT\.UTF-8.*\)/\1/' \
	-e 's/^# \(ko_KR\.UTF-8.*\)/\1/' \
	-e 's/^# \(pl_PL\.UTF-8.*\)/\1/' \
	-e 's/^# \(uk_UA\.UTF-8.*\)/\1/' \
	-e 's/^# \(zh_CN\.UTF-8.*\)/\1/' \
	/etc/locale.gen \
  && locale-gen

WORKDIR /Slic3r
ADD docker/getLatestPrusaSlicerRelease.sh /Slic3r
RUN chmod +x /Slic3r/getLatestPrusaSlicerRelease.sh

RUN apt-get update && apt-get install -y \
  jq \
  curl \
  ca-certificates \
  unzip \
  bzip2 \
  git \
  --no-install-recommends \
  && latestSlic3r=$(/Slic3r/getLatestPrusaSlicerRelease.sh url) \
  && slic3rReleaseName=$(/Slic3r/getLatestPrusaSlicerRelease.sh name) \
  && curl -sSL ${latestSlic3r} > ${slic3rReleaseName} \
  && rm -f /Slic3r/releaseInfo.json \
  && mkdir -p /Slic3r/slic3r-dist \
  && tar -xjf ${slic3rReleaseName} -C /Slic3r/slic3r-dist --strip-components 1 \
  && rm -f /Slic3r/${slic3rReleaseName} \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get purge -y --auto-remove jq unzip bzip2 \
  && apt-get autoclean

ENV PATH="/Slic3r/slic3r-dist:${PATH}"

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o main .

CMD ["./main"]
