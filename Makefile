# Runs all tests
.PHONY: test
test: 
	@astro dev pytest

test-docker:
	@docker-compose up test

.PHONY: bump-patch
bump-patch:
	@bump2version patch
	@git push --tags
	@git push

.PHONY: bump-minor
bump-minor:
	@bump2version minor
	@git push --tags
	@git push

.PHONY: bump-major
bump-major:
	@bump2version major
	@git push --tags
	@git push

.PHONY: start-astro
start-astro:
	@astro dev start

.PHONY: stop-astro
stop-astro:
	@astro dev stop