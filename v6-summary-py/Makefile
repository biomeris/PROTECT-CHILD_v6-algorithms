VANTAGE6_VERSION ?= 5.0.0
TAG ?= latest
REGISTRY ?= ghcr.io/vantage6
PLATFORMS ?= linux/amd64
IMAGE ?= ${REGISTRY}/algorithm/summary

VANTAGE6_MAJOR := $(firstword $(subst ., ,$(VANTAGE6_VERSION)))

INCLUDE_V6_MAJOR_TAG ?= true

PUSH_REG ?= false

_condition_push :=
ifeq ($(PUSH_REG), true)
	_condition_push := not_empty_so_true
endif

.PHONY: image
image:
	@set -e; \
	echo "Building ${IMAGE}:${TAG}-v6-${VANTAGE6_VERSION}"; \
	echo "Building ${IMAGE}:latest"; \
	EXTRA_MAJOR=""; \
	if [ "$(INCLUDE_V6_MAJOR_TAG)" = true ]; then \
	  echo "Building ${IMAGE}:${VANTAGE6_MAJOR}"; \
	  EXTRA_MAJOR='--tag ${IMAGE}:${VANTAGE6_MAJOR}'; \
	fi; \
	docker buildx build \
		--tag ${IMAGE}:${TAG}-v6-${VANTAGE6_VERSION} \
		--tag ${IMAGE}:${TAG} \
		--tag ${IMAGE}:latest \
		$$EXTRA_MAJOR \
		--platform ${PLATFORMS} \
		-f ./Dockerfile \
		$(if ${_condition_push},--push .,.)
