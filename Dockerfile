FROM python:3.9.16-slim as builder

ENV MAKEFLAGS="-j$(nproc)"
ADD requirements.txt /tmp/requirements.txt

RUN pip install --trusted-host 10.235.206.105 --index-url http://10.235.206.105:8888/nexus/repository/python-proxy/simple/ --upgrade pip \
    && pip install --trusted-host 10.235.206.105 --index-url http://10.235.206.105:8888/nexus/repository/python-proxy/simple/ -r /tmp/requirements.txt



FROM python:3.9.16-slim

WORKDIR /app
ADD src src
ADD main.py main.py

COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

RUN apt update \
    && apt upgrade -y \
    && apt install -y tzdata openssh-client \
    && mkdir /root/.ssh \
    && ln -fs /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime \
    && apt clean

CMD ["python","main.py"]