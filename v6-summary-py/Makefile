VANTAGE6_VERSION ?= 4.0.0
TAG ?= latest
REGISTRY ?= harbor2.vantage6.ai
PLATFORMS ?= linux/amd64

# Use `make PUSH_REG=true` to push images to registry after building
PUSH_REG ?= false

# We use a conditional (true on any non-empty string) later. To avoid
# accidents, we don't use user-controlled PUSH_REG directly.
# See: https://www.gnu.org/software/make/manual/html_node/Conditional-Functions.html
_condition_push :=
ifeq ($(PUSH_REG), true)
	_condition_push := not_empty_so_true
endif

image:
	@echo "Building ${REGISTRY}/algorithms/summary:${TAG}-v6-${VANTAGE6_VERSION}"
	@echo "Building ${REGISTRY}/algorithms/summary:latest"
	docker buildx build \
		--tag ${REGISTRY}/algorithms/summary:${TAG}-v6-${VANTAGE6_VERSION} \
		--tag ${REGISTRY}/algorithms/summary:${TAG} \
		--platform ${PLATFORMS} \
		-f ./Dockerfile \
		$(if ${_condition_push},--push .,.)