PACKAGE         ?= types
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR            = $(shell pwd)/rebar3
MAKE						 = make

.PHONY: rel deps test eqc plots

all: compile

##
## Compilation targets
##

compile:
	$(REBAR) compile

clean: packageclean
	$(REBAR) clean

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

##
## Test targets
##

check: test xref dialyzer lint

test: eunit ct proper

eunit:
	${REBAR} as test eunit

ct:
	${REBAR} as test ct

proper:
	${REBAR} as test proper

lint:
	${REBAR} as lint lint

bench: compile
	bin/state_bench.sh

shell:
	${REBAR} shell --apps types

include tools.mk
