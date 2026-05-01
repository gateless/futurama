.PHONY: repl test clean deploy install format-check format-fix

TEST_CLOJURE_ALIAS ?= clojure-1.12
TEST_CORE_ASYNC_ALIAS ?= core.async-1.9

REPL_CLOJURE_ALIAS ?= clojure-1.12
REPL_CORE_ASYNC_ALIAS ?= core.async-1.9

SHELL := /bin/bash
JAVA_OPTS ?= -Dfuturama.executor-factory=clojure.core.async.impl.dispatch/executor-for

export _JAVA_OPTIONS := $(JAVA_OPTS)

env:
	env

repl:
	clojure -M:$(REPL_CLOJURE_ALIAS):$(REPL_CORE_ASYNC_ALIAS):dev:test:app:repl

repl-next: REPL_CORE_ASYNC_ALIAS := core.async-1.9
repl-next: repl

test:
	clojure -M:$(TEST_CLOJURE_ALIAS):$(TEST_CORE_ASYNC_ALIAS):dev:test:app:runner \
		--focus :unit --reporter kaocha.report/documentation --no-capture-output

test-next: TEST_CORE_ASYNC_ALIAS := core.async-1.9
test-next: test

clean:
	rm -rf target build

lint:
	clojure -M:dev:test:app:clj-kondo --copy-configs --dependencies --parallel --lint "$(shell clojure -A:dev:test -Spath)"
	clojure -M:dev:test:app:clj-kondo --lint "src:test" --fail-level "error"

build:
	clojure -X:jar :sync-pom true :jar "build/futurama.jar"

build-native: build
	mkdir -p build/graalvm-config
	clojure -X:uberjar :sync-pom false :jar "build/futurama-test-app.jar"
	java -agentlib:native-image-agent=config-output-dir=build/graalvm-config -jar "build/futurama-test-app.jar" futurama.main
	native-image -jar "build/futurama-test-app.jar" \
		"-H:+ReportExceptionStackTraces" \
		"-H:+JNI" \
		"-H:EnableURLProtocols=http,https,jar" \
		"-J-Dclojure.spec.skip-macros=true" \
		"-J-Dclojure.compiler.direct-linking=true" \
		"--report-unsupported-elements-at-runtime" \
		"--verbose" \
		"--no-fallback" \
		"--no-server" \
		"--allow-incomplete-classpath" \
		"--trace-object-instantiation=java.lang.Thread" \
		"--features=clj_easy.graal_build_time.InitClojureClasses" \
		"-H:ConfigurationFileDirectories=build/graalvm-config" \
		"build/futurama-test-app"

deploy: clean build
	clojure -X:deploy-maven

install:
	clojure -X:install-maven

format-check:
	clojure -M:format-check

format-fix:
	clojure -M:format-fix
