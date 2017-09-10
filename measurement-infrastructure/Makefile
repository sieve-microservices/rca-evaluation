all: build

RANCHER_STACK_NAME:=openstack-kolla-measurement
WILDCARD:=*
EXTENSIONS_LIST = "~"

deploy: upload
	ruby scripts/convert-compose-v2.rb docker-compose.yml > rancher-compose/${RANCHER_STACK_NAME}/docker-compose.yml
	cd rancher-compose/${RANCHER_STACK_NAME}/ && sudo -E rancher-compose --debug --url $(RANCHER_URL) --access-key $(RANCHER_ACCESS_KEY) --secret-key $(RANCHER_SECRET_KEY) up --upgrade --pull --batch-size 99 &

upload: build
	ruby scripts/upload-container.rb docker-compose.yml

build: .build

.build: docker-compose.yml $(wildcard */Dockerfile)
	docker-compose build
	touch .build

clean: folder-clean
	rm -r .build

folder-clean:
	for ext in ${EXTENSIONS_LIST}; do \
		find . -iname "${WILDCARD}$${ext}" -delete; \
	done
	echo "files with 'junk' file extensions deleted"

.PHONY: clean folder-clean
