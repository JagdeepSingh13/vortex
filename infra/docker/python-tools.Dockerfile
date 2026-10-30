# Python image for contract tests (ADR 0007); later reused by the simulator tooling.
# python:3.12-slim pinned by digest, packages pinned in python-tools.requirements.txt.
FROM python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

COPY python-tools.requirements.txt /tmp/requirements.txt
RUN pip install --root-user-action=ignore -r /tmp/requirements.txt

WORKDIR /src
