EXEC_PATH := ./swift/build/search_app
BIN_PATH  := lightning_search.bin
DATA_PATH := ../../data/enwiki_small.csv

build:
	zig build shared
	mkdir -p src/swift/build
	swiftc -O swift/main.swift \
		-sdk "$(shell xcrun --show-sdk-path)" \
		-o swift/build/search_app \
		-L zig-out/lib \
		-lsearch_app \
		-Xlinker -rpath -Xlinker @executable_path/
	mv zig-out/lib/libsearch_app.dylib swift/build/

exec:
	$(EXEC_PATH)

run: build exec

app:
	zig build
	mv zig-out/bin/$(BIN_PATH) web_app/
	cd web_app && ./$(BIN_PATH) $(DATA_PATH)

app_release:
	zig build -Doptimize=ReleaseSafe
	mv zig-out/bin/$(BIN_PATH) web_app/
	cd web_app && ./$(BIN_PATH) $(DATA_PATH)

app_release_fast:
	zig build -Doptimize=ReleaseFast
	mv zig-out/bin/$(BIN_PATH) web_app/
	cd web_app && ./$(BIN_PATH) $(DATA_PATH)

debug:
	lldb $(EXEC_PATH)
