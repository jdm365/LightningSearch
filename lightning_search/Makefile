BIN_PATH  := lightning_search.bin
DATA_PATH := ../../data/mb.csv

install:
	## zig build shared -Doptimize=ReleaseFast
	zig build -Doptimize=ReleaseFast
	## zig build -Doptimize=ReleaseSafe
	## zig build -Doptimize=Debug
	mv zig-out/lib/liblightning_search.so ./python/lightning_search/
	mv zig-out/include/ffi.h ./python/lightning_search/ffi.h
	uv pip uninstall lightning_search
	cd python && uv pip install -n -e .

bench:
	zig build -Doptimize=ReleaseFast
	perf record ./zig-out/bin/$(BIN_PATH)
	perf report

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
