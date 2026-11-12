# syntax=docker/dockerfile:1
#
# The `postgres+pgvectorscale` server: the `postgres+pgvector` image, unchanged,
# with the official pgvectorscale 0.9.1 package installed. Nothing else is added,
# so the only difference between the two is the extension.
#
# crud-bench builds this image itself before the run. It needs no build context.

# The release archive for each architecture, checked against its SHA-256
FROM busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec AS fetch-amd64
ADD --checksum=sha256:63f244a4a4ff356d972137a3c501f9359420d1bc5fdc7e0e2bafd138feaac245 \
	https://github.com/timescale/pgvectorscale/releases/download/0.9.1/pgvectorscale-0.9.1-pg18-amd64.zip /pgvectorscale.zip

FROM busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec AS fetch-arm64
ADD --checksum=sha256:6b15969f137f59787f9e84892f3cf0782fbc32b932217dd8ed07db3f985b0f96 \
	https://github.com/timescale/pgvectorscale/releases/download/0.9.1/pgvectorscale-0.9.1-pg18-arm64.zip /pgvectorscale.zip

# The package, without its debug symbols
FROM fetch-${TARGETARCH} AS fetch
ARG TARGETARCH
RUN unzip -d / /pgvectorscale.zip pgvectorscale-postgresql-18_0.9.1-Linux_${TARGETARCH}.deb \
	&& mv /pgvectorscale-postgresql-18_0.9.1-Linux_${TARGETARCH}.deb /pgvectorscale.deb

# The same image digest as `postgres+pgvector`
FROM pgvector/pgvector:0.8.7-pg18@sha256:c29f30688a9ad961691bdf0ec1650717cc132a5814619d80f80463d6e4c42e47
RUN --mount=from=fetch,source=/pgvectorscale.deb,target=/tmp/pgvectorscale.deb \
	dpkg -i /tmp/pgvectorscale.deb
