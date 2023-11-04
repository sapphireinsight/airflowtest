#!/bin/bash

function setup_kubectl() {
    curl -LO "https://dl.k8s.io/release/v1.23.0/bin/linux/amd64/kubectl"
    chmod +x kubectl
}

function setup_helm() {
    rm -f ./helm
    rm -rf helm*
    wget https://get.helm.sh/helm-v3.9.0-linux-amd64.tar.gz
    tar xzvf helm-v3.9.0-linux-amd64.tar.gz
    ln -s ./linux-amd64/helm helm
    chmod +x helm
}

function setup_kubeconfig() {
    git_username=$1
    git_token=$2

    rm -rf k8s-kubeconfig
    git clone https://${git_username}:${git_token}@git.corp.adobe.com/adobe-platform/k8s-kubeconfig.git
}

function get_eko_token() {
    app_tenant_id=$1
    app_client_id=$2
    app_client_secret=$3

    EKO_RESOURCE_ID="844f28e4-25c1-4f2d-966a-f8ad7bfd459f"

    echo `curl -s -X "POST" "https://login.microsoftonline.com/${app_tenant_id}/oauth2/token?api-version=1.0" \
                            -H "Cookie: flight-uxoptin=true; stsservicecookie=ests; x-ms-gateway-slice=productionb; stsservicecookie=ests" \
                            -H "Content-Type: application/x-www-form-urlencoded" \
                            --data-urlencode "grant_type=client_credentials" \
                            --data-urlencode "client_id=${app_client_id}" \
                            --data-urlencode "resource=${EKO_RESOURCE_ID}" \
                            --data-urlencode "client_secret=${app_client_secret}" \
                            | jq -r '.access_token'`
}

function get_k8s_token() {
    app_tenant_id=$1
    app_client_id=$2
    app_client_secret=$3
    
    K8S_RESOURCE_ID="41abc0c6-712d-4619-9b79-b96da7ffa825"
    
    echo `curl -s -X "POST" "https://login.microsoftonline.com/${app_tenant_id}/oauth2/token?api-version=1.0" \
                            -H "Cookie: flight-uxoptin=true; stsservicecookie=ests; x-ms-gateway-slice=productionb; stsservicecookie=ests" \
                            -H "Content-Type: application/x-www-form-urlencoded" \
                            --data-urlencode "grant_type=client_credentials" \
                            --data-urlencode "client_id=${app_client_id}" \
                            --data-urlencode "resource=${K8S_RESOURCE_ID}" \
                            --data-urlencode "client_secret=${app_client_secret}" \
                            | jq -r '.access_token'`
}

