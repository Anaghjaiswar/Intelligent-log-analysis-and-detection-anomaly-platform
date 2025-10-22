FROM edoburu/pgbouncer:latest

# --- Build-time arguments ---
# These will be passed in from docker-compose.yml
ARG POSTGRES_USER
ARG POSTGRES_PASSWORD

# --- Build the configuration files ---
USER root

# 1. Create the main pgbouncer.ini file
RUN echo "[pgbouncer]" > /etc/pgbouncer/pgbouncer.ini && \
    echo "listen_addr = 0.0.0.0" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "listen_port = 6432" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "auth_type = md5" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "auth_file = /etc/pgbouncer/userlist.txt" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "admin_users = ${POSTGRES_USER}" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "stats_users = ${POSTGRES_USER}" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "pool_mode = session" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "max_client_conn = 500" >> /etc/pgbouncer/pgbouncer.ini && \ 
    echo "default_pool_size = 100" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "[databases]" >> /etc/pgbouncer/pgbouncer.ini && \
    echo "logdb = host=db port=5432 dbname=logdb user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" >> /etc/pgbouncer/pgbouncer.ini

# 2. Create the userlist.txt for client authentication
RUN echo "\"${POSTGRES_USER}\" \"${POSTGRES_PASSWORD}\"" > /etc/pgbouncer/userlist.txt

# --- Final image setup ---
USER postgres
CMD ["/usr/bin/pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]