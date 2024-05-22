#!/usr/bin/env bash

CONTAINER_NAME=harbor.solumesl.com/aims-saas/aims-saas-adapter-sduk-py:0.0.6

find . -type d -name __pycache__ -print0 | xargs -0 -I DIR rm -r DIR

docker build -t "${CONTAINER_NAME}" .

docker push "${CONTAINER_NAME}"

kubectl -n aims-saas-adapter-sduk set image deployments aims-saas-adapter-sduk-py aims-saas-adapter-sduk-py="$CONTAINER_NAME"

kubectl -n aims-saas-adapter-sduk rollout restart deploy aims-saas-adapter-sduk-py
