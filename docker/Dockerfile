# multi-stage optimization based on https://stackoverflow.com/a/71088501/1469797

FROM python:3 AS build

RUN python3 -m venv /venv
ENV PATH=/venv/bin:$PATH

WORKDIR /app

COPY . .

RUN pip install .

FROM python:3-slim

COPY --from=build /venv /venv
ENV PATH=/venv/bin:$PATH

WORKDIR /app

COPY --from=build /app /app

CMD ["/bin/sh"]
