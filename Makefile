# minimalistic utility to test and develop locally

SHELL = /bin/sh
.DEFAULT_GOAL := help
MAKEFLAGS += -j2

export DOCKER_IMAGE_NAME ?= osparc-meta-parallelrunner
export DOCKER_IMAGE_TAG ?= 0.1.4

export MASTER_AWS_REGISTRY ?= registry.osparc-master-zmt.click
export MASTER_REGISTRY ?= registry.osparc-master.speag.com
export STAGING_REGISTRY ?= registry.osparc.speag.com
export LOCAL_REGISTRY ?= registry:5000

define _bumpversion
	# upgrades as $(subst $(1),,$@) version, commits and tags
	@docker run -it --rm -v $(PWD):/${DOCKER_IMAGE_NAME} \
		-u $(shell id -u):$(shell id -g) \
		itisfoundation/ci-service-integration-library:v1.0.1-dev-33 \
		sh -c "cd /${DOCKER_IMAGE_NAME} && bump2version --verbose --list --config-file $(1) $(subst $(2),,$@)"
endef

.PHONY: version-patch version-minor version-major
version-patch version-minor version-major: .bumpversion.cfg ## increases service's version
	@make compose-spec
	@$(call _bumpversion,$<,version-)
	@make compose-spec

.PHONY: compose-spec
compose-spec: ## runs ooil to assemble the docker-compose.yml file
	@docker run --rm -v $(PWD):/${DOCKER_IMAGE_NAME} \
		-u $(shell id -u):$(shell id -g) \
		itisfoundation/ci-service-integration-library:v1.0.1-dev-33 \
		sh -c "cd /${DOCKER_IMAGE_NAME} && ooil compose"

clean:
	rm -rf docker-compose.yml

.PHONY: build
build: clean compose-spec	## build docker image
	chmod -R 755 docker_scripts
	docker compose build

validation-clean:
	sudo rm -rf validation-tmp
	cp -r validation validation-tmp
	chmod -R 770 validation-tmp

validation_client_run: validation-clean
	pip install osparc-filecomms
	VALIDATION_CLIENT_INPUT_PATH=validation-tmp/outputs/output_1 VALIDATION_CLIENT_OUTPUT_PATH=validation-tmp/inputs/input_2 python validation-client/client.py

docker_compose: validation-clean
	docker compose down
	docker compose --file docker-compose-local.yml up
	
.PHONY: run-local
run-local: validation_client_run docker_compose	## runs image with local configuration


.PHONY: publish-local
publish-local: ## push to local throw away registry to test integration
	docker tag simcore/services/dynamic/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} $(LOCAL_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	docker push $(LOCAL_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	@curl $(LOCAL_REGISTRY)/v2/_catalog | jq

.PHONY: publish-master
publish-master: ## push to local throw away registry to test integration
	docker tag simcore/services/dynamic/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} $(MASTER_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	docker push $(MASTER_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	@curl $(MASTER_REGISTRY)/v2/_catalog | jq

.PHONY: publish-staging
publish-staging: ## push to local throw away registry to test integration
	docker tag simcore/services/dynamic/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} $(STAGING_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	docker push $(STAGING_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	@curl $(STAGING_REGISTRY)/v2/_catalog | jq

.PHONY: publish-master-aws
publish-master-aws: ## push to local throw away registry to test integration
	docker tag simcore/services/dynamic/${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} $(MASTER_AWS_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	docker push $(MASTER_AWS_REGISTRY)/simcore/services/dynamic/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
	@curl $(MASTER_AWS_REGISTRY)/v2/_catalog | jq

.PHONY: help
help: ## this colorful help
	@echo "Recipes for '$(notdir $(CURDIR))':"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[[:alpha:][:space:]_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
