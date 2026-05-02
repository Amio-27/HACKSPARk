FROM busybox:1.36.1 AS runtime
WORKDIR /www
COPY index.html ./
COPY status ./status
RUN for route in login register products trending availability chat surge-calendar recommendations; do \
  mkdir -p "$route" && cp index.html "$route/index.html"; \
done
EXPOSE 3000
CMD ["httpd", "-f", "-p", "3000", "-h", "/www"]