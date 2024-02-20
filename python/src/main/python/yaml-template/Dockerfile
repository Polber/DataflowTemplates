#========================================================================#
# Create build environment from base Python 3.11 template launcher image #
#========================================================================#
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base as python-base

# Copy template files to /template
ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}
COPY main.py /template
COPY requirements.txt /template
WORKDIR ${WORKDIR}

ARG REQUIREMENTS_FILE=requirements.txt

# pip install dependencies and cache wheels
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -U -r ${REQUIREMENTS_FILE} \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r ${REQUIREMENTS_FILE}

# Compress Python to reduce image size
# RUN apt-get update && apt-get install lrzip -y
# RUN cd /usr/local/lib/ && lrztar python3.11 && mv python3.11.tar.lrz /template

# Remove unnecessary template launchers
RUN rm /opt/google/dataflow/go_template_launcher /opt/google/dataflow/java_template_launcher
# RUN rm /lib/x86_64-linux-gnu/libm-2.31.so

# Name startup script python_template_launcher to trick dataflow to run startup script first
# RUN mv /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher_main
# COPY startup.sh /opt/google/dataflow/python_template_launcher


#============================================================#
# Create Distroless xlang image compatible with YamlTemplate #
#============================================================#
FROM gcr.io/dataflow-templates-base/java11-template-launcher-base-distroless

# Set python environment variables
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=${REQUIREMENTS_FILE}
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=main.py
ENV PIP_NO_DEPS=True
# ENV PYTHONPATH=/usr/local/lib/python3.11/site-packages
# ENV PYTHONHOME=/usr/local
# ENV PYTHONPATH=/usr/local/lib/python3.11/site-packages

ARG CHIPSET_ARCH=x86_64-linux-gnu

# Copy necessary template and launcher files from build env
COPY --from=python-base /template /template
COPY --from=python-base /tmp/dataflow-requirements-cache /tmp/dataflow-requirements-cache
COPY --from=python-base /opt/google /opt/google
COPY --from=python-base /usr/local/lib/python3.11 /usr/local/lib/python3.11

# Copy lrzip package and dependencies from build env
# COPY --from=python-base /usr/bin/lrzcat /usr/local/bin
# COPY --from=python-base /usr/bin/lrzuntar /usr/local/bin
# COPY --from=python-base /usr/bin/lrzip /usr/local/bin
# COPY --from=python-base /bin/tar /bin/tar
# COPY --from=python-base /usr/bin/basename /usr/bin/basename
# COPY --from=python-base /lib/${CHIPSET_ARCH}/libexpat* /lib/${CHIPSET_ARCH}/
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libacl* /lib/${CHIPSET_ARCH}/
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libpcre2* /lib/${CHIPSET_ARCH}/

# Copy python and dependencies from build env
COPY --from=python-base /usr/local/lib/lib* /usr/local/lib/
COPY --from=python-base /usr/local/bin/python /usr/local/bin/python
COPY --from=python-base /etc/ld.so.cache /etc/ld.so.cache
COPY --from=python-base /lib/${CHIPSET_ARCH}/lib* /lib/${CHIPSET_ARCH}/
COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/liblz* /lib/${CHIPSET_ARCH}/
COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libffi* /usr/lib/${CHIPSET_ARCH}/

# Copy minimal commands needed to execute template
COPY --from=python-base /bin/sh /bin/sh
# COPY --from=python-base /bin/ls /bin/ls
COPY --from=python-base /usr/bin/which /usr/bin/which

# Remove java launcher that comes with java distroless container
# COPY --from=python-base /bin/rm /bin/rm
# RUN rm -r /opt/google/dataflow/java_template_launcher

# Install bash and copy startup script responsible for uncompressing python
# COPY --from=python-base /bin/bash /bin/bash

WORKDIR /template

# ENTRYPOINT ["/template/startup.sh"]
# ENTRYPOINT ["/bin/sh"]
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]