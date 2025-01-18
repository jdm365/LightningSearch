SHELL := /bin/bash
SDK_PATH := $(shell xcrun --show-sdk-path)
EXEC_PATH := ./src/swift/build/search_app

build:
	zig build shared
	mkdir -p src/swift/build
	swiftc src/swift/main.swift \
		-sdk "$(SDK_PATH)" \
		-o src/swift/build/search_app \
		-L zig-out/lib \
		-lsearch_app \
		-Xlinker -rpath -Xlinker @executable_path/
	mv zig-out/lib/libsearch_app.dylib src/swift/build/

exec:
	$(EXEC_PATH)

run: build exec

debug:
	lldb $(EXEC_PATH)
