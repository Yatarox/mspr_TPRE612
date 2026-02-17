FROM mysql:8.0

ENV MYSQL_ROOT_PASSWORD=root
ENV MYSQL_DATABASE=rail_dw

COPY schema.sql /docker-entrypoint-initdb.d/01-schema.sql
COPY init_countries.sql /docker-entrypoint-initdb.d/02-init_countries.sql

EXPOSE 3306