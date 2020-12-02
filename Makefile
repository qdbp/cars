.PHONY: install

install:
	chgrp -R nginx ./
	cp cars.conf /etc/
	cp cars.service /etc/systemd/system/
	cp nginx_server_dropin.conf /etc/nginx/cars_server.conf