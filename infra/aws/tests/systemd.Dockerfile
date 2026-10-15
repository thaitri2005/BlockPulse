# Amazon Linux 2023 with systemd as PID 1, for rehearsing the collector's services (see systemd_e2e.sh).
FROM public.ecr.aws/amazonlinux/amazonlinux:2023
RUN dnf install -y -q --setopt=install_weak_deps=False systemd shadow-utils util-linux procps-ng findutils \
        python3.11 python3.11-pip \
    && dnf clean all
STOPSIGNAL SIGRTMIN+3
CMD ["/usr/sbin/init"]
