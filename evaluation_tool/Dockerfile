FROM python:3.11-slim AS base

ENV TZ=Asia/Taipei

WORKDIR /app

# ======= install python packages =======
FROM base AS builder

RUN pip install -U pip

# Install packages
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=/requirements.txt \
    CFLAGS="-Wno-narrowing" pip install --prefix=/runtime --src=/runtime/src --no-warn-script-location --force-reinstall -r /requirements.txt

# ======= build final image =======
FROM base AS final

COPY --link --from=builder /runtime /usr/local

# copy codebase
COPY --link . .

EXPOSE 8501
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
