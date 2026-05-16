FROM grafana/grafana:latest

USER root

# Copier les configurations
COPY provisioning /etc/grafana/provisioning
COPY dashboards /var/lib/grafana/dashboards

# L'utilisateur dans l'image Grafana est 472 (grafana)
RUN chown -R 472:472 /etc/grafana/provisioning && \
    chown -R 472:472 /var/lib/grafana/dashboards

USER grafana