#
# Copyright (C) 2025 Marcio Endo.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Rinha de Backend
#

## Coordinates
GROUP_ID := br.dev.o7.marcio
ARTIFACT_ID := demo.rinha
VERSION := 002

## JDK 24 required
JAVA_RELEASE := 24

## Structure Concurrency
ENABLE_PREVIEW := 1

## use native by default
## use ENABLE_JLINK=1 to override
ENABLE_NATIVE := 1

## Test dependencies
SLF4J_NOP := org.slf4j/slf4j-nop/2.0.17
TESTNG := org.testng/testng/7.11.0

# Delete the default suffixes
.SUFFIXES:

#
# rinha
#

.PHONY: all
all: test front-native back-native

include make/java-core.mk

#
# rinha@clean
#

include make/common-clean.mk

#
# rinha@compile
#

include make/java-compile.mk

#
# rinha@test-compile
#

## test compile deps
TEST_COMPILE_DEPS := $(TESTNG)

include make/java-test-compile.mk

#
# rinha@test
#

## test main class
TEST_MAIN := demo.rinha.StartTest

## www test runtime dependencies
TEST_RUNTIME_DEPS := $(SLF4J_NOP)

## test --add-modules
TEST_ADD_MODULES := org.testng
TEST_ADD_MODULES += org.slf4j

## test --add-reads
TEST_ADD_READS := demo.rinha=org.testng

include make/java-test.mk

#
# native related tasks
#

## defines module name
MODULE := $(ARTIFACT_ID)

## configures NATIVE_IMAGE
ifdef GRAALVM_HOME
NATIVE_IMAGE := $(GRAALVM_HOME)/bin/native-image
else
$(error Required GRAALVM_HOME variable was not set)
endif

## native-image common args
NATIVE_IMAGEX := $(NATIVE_IMAGE)
NATIVE_IMAGEX += -march=skylake
ifeq ($(ENABLE_PREVIEW),1)
NATIVE_IMAGEX += --enable-preview
endif
NATIVE_IMAGEX += --module-path $(CLASS_OUTPUT)

## native macro
define native
## *-native filename
$(1)_FILENAME := $(2)-$$(VERSION)

## *-native output file
$(1)_OUTPUT_FILE := $$(WORK)/$$($(1)_FILENAME)

## *-native native-image command
$(1)_IMAGEX := $$(NATIVE_IMAGEX)
$(1)_IMAGEX += -o $$($(1)_OUTPUT_FILE)
$(1)_IMAGEX += --module $$(MODULE)/$$($(1)_MAIN)

.PHONY: $(2)-native
$(2)-native: $$($(1)_OUTPUT_FILE)

.PHONY: $(2)-native-clean
$(2)-native-clean:
	rm -f $$($(1)_OUTPUT_FILE)
	
$$($(1)_OUTPUT_FILE): $$(COMPILE_MARKER)
	$$($(1)_IMAGEX)
endef

#
# rinha@front-native
#

## front-native main class
FNATIVE_MAIN := demo.rinha.Front

$(eval $(call native,FNATIVE,front))

#
# rinha@back-native
#

## back-native main class
BNATIVE_MAIN := demo.rinha.Back

$(eval $(call native,BNATIVE,back))


#
# rinha link related tasks
#

## jlink macro
define jlink
## *-jlink output dir
$(1) := $$(WORK)/$(2)-jlink-$$(VERSION)

## *-jlink command
$(1)X := $$(JLINK)
$(1)X += --add-modules $$(MODULE)
ifeq ($(ENABLE_DEBUG),1)
$(1)X += --add-modules jdk.jdi,jdk.jdwp.agent
endif
$(1)X += --compress zip-9
$(1)X += --module-path $$(CLASS_OUTPUT):$$(JAVA_HOME)/jmods
$(1)X += --no-header-files
$(1)X += --output $$($(1))
$(1)X += --verbose

## jlink marker
$(1)_MARKER = $$($(1))-marker

.PHONY: $(2)-jlink
$(2)-jlink: $$($(1)_MARKER)

.PHONY: $(2)-jlink-clean
$(2)-jlink-clean:
	rm -rf $$($(1)) $$($(1)_MARKER)

$$($(1)_MARKER): $$(COMPILE_MARKER)
	$$($(1)X)
	touch $$@
endef

#
# rinha@front-jlink
#

$(eval $(call jlink,FJLINK,front))

#
# rinha@back-jlink
#

$(eval $(call jlink,BJLINK,back))

#
# rinha@jlink-clean
#

.PHONY: jlink-clean
jlink-clean: front-jlink-clean back-jlink-clean

#
# docker related tasks
#

## docker macro
define docker
## *-dockerfile
$(1) := $$(WORK)/$(2)-dockerfile

## *-dockerfile tag
$(1)_TAG := ghcr.io/marcioendo/rinha2025-$(2):v$$(VERSION)

## *-docker command
$(1)_BUILDX := docker build
$(1)_BUILDX += --progress=plain
$(1)_BUILDX += --file $$($(1))
$(1)_BUILDX += --tag $$($(1)_TAG)
$(1)_BUILDX += .

## *-dockerfile marker
$(1)_MARKER := $$($(1))-marker

## *-docker req
ifndef $(1)_REQ
$$(error missing $(1)_REQ variable)
endif

## *-docker reqs
$(1)_REQS := $$($(1)_REQ)
$(1)_REQS += $$($(1))

## *-docker copy
ifndef $(1)_COPY
$$(error missing $(1)_COPY variable)
endif

## *-docker entrypoint
ifndef $(1)_ENTRYPOINT
$$(error missing $(1)_ENTRYPOINT variable)
endif

## *-dockerfile contents
define $(1)_CONTENTS :=
FROM debian:bookworm-slim

LABEL org.opencontainers.image.source=https://github.com/marcioendo/demo.rinha

$$($(1)_COPY)

ENTRYPOINT [$$($(1)_ENTRYPOINT)]
endef

## *-dockerfile targets
.PHONY: $(2)-docker
$(2)-docker: $$($(1)_MARKER)

.PHONY: $(2)-docker-clean
$(2)-docker-clean:
	rm -f $$($(1)) $$($(1)_MARKER)
	
$$($(1)_MARKER): $$($(1)_REQS)
	$$($(1)_BUILDX)
	touch $$@

$$($(1)): Makefile
	$$(file > $$($(1)),$$($(1)_CONTENTS))
endef

#
# rinha@front-docker
#

ifndef ENABLE_JLINK
## front-docker req
FDOCKER_REQ := $(FNATIVE_OUTPUT_FILE)

## front-docker copy
FDOCKER_COPY := COPY $(FNATIVE_OUTPUT_FILE) /front

## front-docker entrypoint
FDOCKER_ENTRYPOINT := "/front"
else
## front-docker req
FDOCKER_REQ := $(FJLINK_MARKER)

## front-docker copy
FDOCKER_COPY := COPY $(FJLINK) front/

## front-docker entrypoint
FDOCKER_ENTRYPOINT := "/front/bin/java"
ifeq ($(ENABLE_DEBUG),1)
FDOCKER_ENTRYPOINT += , "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:7000"
endif
ifeq ($(ENABLE_PREVIEW),1)
FDOCKER_ENTRYPOINT += , "--enable-preview"
endif
FDOCKER_ENTRYPOINT += , "--module", "$(MODULE)/demo.rinha.Front"
endif

$(eval $(call docker,FDOCKER,front))

#
# rinha@back-docker
#

ifndef ENABLE_JLINK
## back-docker req
BDOCKER_REQ := $(BNATIVE_OUTPUT_FILE)

## back-docker copy
BDOCKER_COPY := COPY $(BNATIVE_OUTPUT_FILE) /back

## back-docker entrypoint
BDOCKER_ENTRYPOINT := "/back"
else
## back-docker req
BDOCKER_REQ := $(BJLINK_MARKER)

## back-docker copy
BDOCKER_COPY := COPY $(BJLINK) back/

## back-docker entrypoint
BDOCKER_ENTRYPOINT := "/back/bin/java"
ifeq ($(ENABLE_DEBUG),1)
BDOCKER_ENTRYPOINT += , "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:7000"
endif
ifeq ($(ENABLE_PREVIEW),1)
BDOCKER_ENTRYPOINT += , "--enable-preview"
endif
BDOCKER_ENTRYPOINT += , "--module", "$(MODULE)/demo.rinha.Back"
endif

$(eval $(call docker,BDOCKER,back))

#
# rinha@compose
#

## compose file
COMPOSE := $(WORK)/compose

## compose up command
COMPOSE_UPX := docker compose 
COMPOSE_UPX += --file $(COMPOSE)
COMPOSE_UPX += up

## compose contents
define COMPOSE_CONTENTS =
services:
  back0:
    image: $(BDOCKER_TAG)
    command: ["0"]
    container_name: back0
    hostname: back0
    volumes:
      - /tmp:/tmp
    ports:
      - "9990:9990"
      $(if $(ENABLE_DEBUG),- "7090:7000")
    networks:
      - backend
      - payment-processor
    deploy:
      resources:
        limits:
          cpus: "0.6"
          memory: "150MB"

  back1:
    image: $(BDOCKER_TAG)
    command: ["1"]
    container_name: back1
    hostname: back1
    volumes:
      - /tmp:/tmp
    ports:
      - "9991:9991"
      $(if $(ENABLE_DEBUG),- "7091:7000")
    networks:
      - backend
      - payment-processor
    deploy:
      resources:
        limits:
          cpus: "0.6"
          memory: "150MB"

  front:
    image: $(FDOCKER_TAG)
    container_name: front
    hostname: front
    volumes:
      - /tmp:/tmp
    depends_on:
      - back0
      - back1
    ports:
      - "9999:9999"
      $(if $(ENABLE_DEBUG),- "7099:7000")
    networks:
      - backend
    deploy:
      resources:
        limits:
          cpus: "0.3"
          memory: "50MB"

networks:
  backend:
    driver: bridge
  payment-processor:
    external: true
endef

.PHONY: compose
compose: $(COMPOSE)
	$(COMPOSE_UPX)
	
.PHONY: compose-clean
compose-clean:
	rm -f $(COMPOSE)

.PHONY: compose-down
compose-down:
	docker compose --file $(COMPOSE) down

$(COMPOSE): Makefile $(FDOCKER_MARKER) $(BDOCKER_MARKER)
	$(file > $(COMPOSE),$(COMPOSE_CONTENTS))

#
# GH related tasks
#

## It should define:
## - GH_REPOS
## - GH_PACKAGE_TOKEN
-include $(HOME)/.config/marcioendo/gh-config.mk

ifndef GH_REPOS
GH_REPOS := /tmp
endif

#
# rinha@gh-pr
#

## local upstream directory
UP := $(GH_REPOS)/rinha-de-backend-2025

## upstream repo
UP_REPO := git@github.com:marcioendo/rinha-de-backend-2025.git

## PR directory
PR := $(UP)/participantes/marcioendo

## PR docker-compose
PR_COMPOSE := $(PR)/docker-compose.yml

## PR info.json
PR_INFO := $(PR)/info.json

## PR README
PR_README := $(PR)/README.md

## PR marker
PR_MARKER := $(WORK)/pr-marker

.PHONY: gh-pr
gh-pr: $(PR_MARKER)

.PHONY: gh-pr-clean
gh-pr-clean:
	rm -f $(PR)/* $(PR_MARKER)

$(PR_MARKER): $(PR_COMPOSE) $(PR_INFO) $(PR_README)
	touch $@

$(PR_COMPOSE): $(COMPOSE) | $(PR)
	cp $< $@

$(PR_INFO): info.json Makefile | $(PR)
	sed -e 's/{{VERSION}}/$(VERSION)/' $< > $@

$(PR_README): README-pr.md Makefile | $(PR)
	sed -e 's/{{VERSION}}/$(VERSION)/' $< > $@

$(PR): | $(UP)
	mkdir --parents $@

$(UP):
	git clone --depth=2 $(UP_REPO) $(UP)
	
#
# rinha@gh-package
#

## GH package marker
GH_PACKAGE_MARKER := $(WORK)/gh-package-marker

.PHONY: gh-package
gh-package: $(GH_PACKAGE_MARKER)

.PHONY: gh-package-clean
gh-package-clean:
	rm -f $(GH_PACKAGE_MARKER)

$(GH_PACKAGE_MARKER): $(COMPOSE)
	@echo $(GH_PACKAGE_TOKEN) | docker login ghcr.io -u marcioendo --password-stdin
	docker push $(FDOCKER_TAG)
	docker push $(BDOCKER_TAG)
	docker logout
	touch $@
	
#
# rinha@gh-release
#

## GH_API
GH_API = https://api.github.com/repos/marcioendo/$(ARTIFACT_ID)

include make/gh-release.mk
