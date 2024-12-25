include ../mks/base_variables.mk

WEB_PORT := 8501
IMAGE_TAG := analytics/evaluation-tool

container:
	docker build -t $(IMAGE_TAG) .

# 開啟 eva-tool debug server
debug-server:
	docker run --rm -it -p $(WEB_PORT):$(WEB_PORT) $(IMAGE_TAG)


# 開啟 eva-tool container 並進入 bash shell
shell:
	docker run --rm -it -p $(WEB_PORT):$(WEB_PORT) $(IMAGE_TAG) bash


## 將 eva-tool image 推到 ECR 上
push-ecr: ECR_LOCAL_TAG := $(IMAGE_TAG):latest
push-ecr: ECR_REMOTE_TAG := $(ECR_REPO)/$(ECR_LOCAL_TAG)
push-ecr: push-ecr
