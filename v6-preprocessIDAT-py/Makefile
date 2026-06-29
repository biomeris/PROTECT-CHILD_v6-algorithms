VANTAGE6_VERSION ?= 5.0.0
TAG ?= latest
IMAGE ?= ghcr.io/vantage6/algorithm/preprocessIDATPLATFORMS ?= linux/amd64

# Major segment of VANTAGE6_VERSION (e.g. 5.0.0 -> 5)
VANTAGE6_MAJOR := $(firstword $(subst ., ,$(VANTAGE6_VERSION)))

# When true, also tag/push ${IMAGE}:${VANTAGE6_MAJOR}. CI sets false on algorithm
# prereleases if that major tag already exists (see vantage6-workflows release).
INCLUDE_V6_MAJOR_TAG ?= true

# Use `make PUSH_REG=true` to push images to registry after building
PUSH_REG ?= false

# We use a conditional (true on any non-empty string) later. To avoid
# accidents, we don't use user-controlled PUSH_REG directly.
# See: https://www.gnu.org/software/make/manual/html_node/Conditional-Functions.html
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
		--tag ${IMAGE}:latest \
		$$EXTRA_MAJOR \
		--platform ${PLATFORMS} \
		-f ./Dockerfile \
		$(if ${_condition_push},--push .,.)
