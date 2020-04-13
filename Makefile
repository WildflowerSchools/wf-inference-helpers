VERSION ?= 0

.PHONY: build build-all


build:
# 	docker build --no-cache -t wildflowerschools/wf-deep-docker:video-queue-feeder-v$(VERSION) -f queue-feeder/Dockerfile .
# 	docker build --no-cache -t wildflowerschools/wf-deep-docker:video-prepare-v$(VERSION) -f prepare/Dockerfile .
# 	docker push wildflowerschools/wf-deep-docker:video-queue-feeder-v$(VERSION)
# 	docker push wildflowerschools/wf-deep-docker:video-prepare-v$(VERSION)
	docker build --no-cache -t wildflowerschools/wf-deep-docker:video-prepare-tooling-v$(VERSION) -f airflow/Dockerfile .
	docker push wildflowerschools/wf-deep-docker:video-prepare-tooling-v$(VERSION)
