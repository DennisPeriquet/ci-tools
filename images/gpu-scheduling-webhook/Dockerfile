FROM quay.io/centos/centos:stream9
LABEL maintainer="dgemoli@redhat.com"

ADD gpu-scheduling-webhook /usr/bin/gpu-scheduling-webhook
ENTRYPOINT ["/usr/bin/gpu-scheduling-webhook"]