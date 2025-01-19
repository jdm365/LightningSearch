import Cocoa
import Foundation
import UniformTypeIdentifiers

// Bridge to C functions
private let searchLib = dlopen(nil, RTLD_NOW)

private let init_allocators = dlsym(searchLib, "init_allocators")
    .map { unsafeBitCast($0, to: (@convention(c) () -> Void).self) }

private let get_handler = dlsym(searchLib, "getQueryHandlerLocal")
    .map { unsafeBitCast($0, to: (@convention(c) () -> UnsafeMutableRawPointer?).self) }

private let search_func = dlsym(searchLib, "search")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeMutableRawPointer?,
        UnsafePointer<CChar>?,
        UnsafeMutablePointer<UInt32>?,
        UnsafeMutablePointer<UInt32>?,
        UnsafeMutablePointer<UInt32>?,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> Void).self) }

private let read_header = dlsym(searchLib, "readHeader")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafePointer<CChar>?
    ) -> Void).self) }

private let get_cols = dlsym(searchLib, "getColumnNames")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>,
        UnsafeMutablePointer<UInt32>
    ) -> Void).self) }

private let get_search_cols = dlsym(searchLib, "getSearchColumns")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafeMutablePointer<CChar>
    ) -> Void).self) }

private let add_search_col = dlsym(searchLib, "addSearchCol")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafePointer<CChar>
    ) -> Void).self) }

private let index_file = dlsym(searchLib, "indexFile")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer
    ) -> Void).self) }

class SearchBridge {
    var queryHandler: UnsafeMutableRawPointer?
    private var resultCount: UInt32 = 0
    private var startPositions = [UInt32](repeating: 0, count: 100 * 3)
    private var lengths = [UInt32](repeating: 0, count: 100 * 3)
    private var resultBuffers = [UnsafeMutablePointer<CChar>?](repeating: nil, count: 100)
    private var numColumns: UInt32 = 0
    var columnNames: [String] = []
    var searchColumnNames: [String] = []
    
    init() {
        if let initFn = init_allocators {
            print("Initializing allocators")
            initFn()
        } else {
            print("Failed to get init_allocators function")
        }

        if let handlerFn = get_handler {
            queryHandler = handlerFn()
            print("QueryHandler initialized: \(String(describing: queryHandler))")
        } else {
            print("Failed to get handler function")
        }
    }

    func getColumnNames() -> Void {
        guard let handler = queryHandler else {
            print("QueryHandler is nil")
            return
        }

        let names = UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>.allocate(capacity: 128)
        defer {
            // First free each string buffer
            for i in 0..<128 {
                if let ptr = names[i] {
                    ptr.deallocate()
                }
            }
            // Then free the array of pointers
            names.deallocate()
        }
        
        // Allocate buffers for each potential string (assuming max length of 256 chars)
        for i in 0..<128 {
            names[i] = UnsafeMutablePointer<CChar>.allocate(capacity: 256)
        }
        
        if let fn = get_cols {
            fn(handler, names, &numColumns)
            
            for i in 0..<Int(numColumns) {
                if let namePtr = names[i] {
                    columnNames.append(String(cString: namePtr))
                }
            }
            return
        }
    }

    func getSearchColumns() -> Void {
        guard let handler = queryHandler else {
            print("QueryHandler is nil")
            return
        }

        let colMask = UnsafeMutablePointer<CChar>.allocate(capacity: columnNames.count)
        defer {
            colMask.deallocate()
        }

        for i in 0..<columnNames.count {
            colMask[i] = 0;
        }

        if let fn = get_search_cols {
            fn(handler, colMask)
            
            for i in 0..<Int(columnNames.count) {
                if colMask[i] == 1 {
                    searchColumnNames.append(columnNames[i])
                }
            }
            return
        }
    }
    
    func performSearch(query: String) -> [[String]] {
        resultCount = 0
        
        print("Query: \(query)")
        var startTime = CFAbsoluteTimeGetCurrent()
        query.withCString { queryStr in
            search_func?(
                queryHandler,
                queryStr,
                &resultCount,
                &startPositions,
                &lengths,
                &resultBuffers
            )
        }
        var timeElapsed = CFAbsoluteTimeGetCurrent() - startTime
        print("Time elapsed: \(timeElapsed) seconds")
        print("Found \(resultCount) results")
        startTime = CFAbsoluteTimeGetCurrent()
        var results: [[String]] = []
        for i in 0..<Int(resultCount) {
            var row: [String] = []
            for j in 0..<Int(numColumns) {
                let start = Int(startPositions[i * Int(numColumns) + j])
                let length = Int(lengths[i * Int(numColumns) + j])
                if let buffer = resultBuffers[i] {
                    let data = Data(bytes: buffer.advanced(by: start), count: length)
                    if let str = String(data: data, encoding: .utf8) {
                        row.append(str)
                    }
                }
            }
            results.append(row)
        }
        timeElapsed = CFAbsoluteTimeGetCurrent() - startTime
        print("Time elapsed populate: \(timeElapsed) seconds")
        
        return results
    }
}

class AppDelegate: NSObject, NSApplicationDelegate, NSTableViewDataSource, NSTableViewDelegate {
    var window: NSWindow!
    var tableView: NSTableView!
    var searchFields: [NSSearchField] = []
    var searchResults: [[String]] = []
    var searchStrings: [String] = []
    let searchBridge = SearchBridge()
    var columnSelectionWindow: NSWindow!
    var checkboxes: [NSButton] = []
    
    func applicationDidFinishLaunching(_ notification: Notification) {
        window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 400, height: 200),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        
        let contentView = NSView(frame: window.contentRect(forFrameRect: window.frame))
        window.contentView = contentView

        let label = NSTextField(frame: NSRect(x: 20, y: 120, width: 360, height: 40))
        label.stringValue = "Please select a CSV file to index"
        label.isEditable = false
        label.isBordered = false
        label.drawsBackground = false
        contentView.addSubview(label)
        
        let button = NSButton(frame: NSRect(x: 150, y: 60, width: 100, height: 32))
        button.title = "Choose File"
        button.bezelStyle = .rounded
        button.target = self
        button.action = #selector(chooseFile)
        contentView.addSubview(button)

        window.title = "File Selector"
        window.center()
        window.makeKeyAndOrderFront(nil)
    }

    @objc func chooseFile() {
        let openPanel = NSOpenPanel()
        openPanel.allowsMultipleSelection = false
        openPanel.canChooseDirectories = false
        openPanel.canChooseFiles = true
        openPanel.allowedContentTypes = [UTType.commaSeparatedText]
        
        openPanel.beginSheetModal(for: window) { [weak self] response in
            guard let self = self else { return }
            if response == .OK, let url = openPanel.url {
                self.readHeader(fileURL: url)
            }
        }
    }

    private func readHeader(fileURL: URL) {
        guard let cPath = fileURL.path.cString(using: .utf8) else {
            print("Failed to convert path to C string")
            return
        }
        guard let handler = searchBridge.queryHandler else {
            print("QueryHandler is nil")
            return
        }
        read_header!(handler, cPath)
        searchBridge.getColumnNames()
        showColumnSelection()
    }

    private func showColumnSelection() {
        columnSelectionWindow = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 300, height: CGFloat(50 + searchBridge.columnNames.count * 30)),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        
        let contentView = NSView(frame: columnSelectionWindow!.contentRect(forFrameRect: columnSelectionWindow!.frame))
        columnSelectionWindow!.contentView = contentView
        
        let label = NSTextField(frame: NSRect(x: 20, y: CGFloat(10 + searchBridge.columnNames.count * 30), width: 260, height: 30))
        label.stringValue = "Select columns to include in search:"
        label.isEditable = false
        label.isBordered = false
        label.drawsBackground = false
        contentView.addSubview(label)
        
        checkboxes.removeAll()
        
        for (index, column) in searchBridge.columnNames.enumerated() {
            let checkbox = NSButton(frame: NSRect(x: 20, y: CGFloat((searchBridge.columnNames.count - 1 - index) * 30 + 20), width: 200, height: 20))
            checkbox.title = column
            checkbox.setButtonType(.switch)
            checkbox.state = .off
            contentView.addSubview(checkbox)
            checkboxes.append(checkbox)
        }
        
        let startButton = NSButton(frame: NSRect(x: 200, y: 20, width: 80, height: 32))
        startButton.title = "Start"
        startButton.bezelStyle = .rounded
        startButton.target = self
        startButton.action = #selector(startIndexing)
        contentView.addSubview(startButton)
        
        columnSelectionWindow!.title = "Select Search Columns"
        columnSelectionWindow!.center()
        window.beginSheet(columnSelectionWindow!) { response in
            // Sheet closed callback
        }
    }

    @objc func startIndexing() {
        guard let handler = searchBridge.queryHandler else {
            print("QueryHandler is nil")
            return
        }

        searchBridge.searchColumnNames = checkboxes.enumerated()
            .filter { $0.element.state == .on }
            .map { searchBridge.columnNames[$0.offset] }
        
        if searchBridge.searchColumnNames.isEmpty {
            let alert = NSAlert()
            alert.messageText = "Please select at least one column"
            alert.alertStyle = .warning
            alert.beginSheetModal(for: columnSelectionWindow!) { _ in }
            return
        }

        for i in 0..<searchBridge.searchColumnNames.count {
            guard let cString = searchBridge.searchColumnNames[i].cString(using: .utf8) else {
                print("Failed to convert column name to C string")
                continue
            }
            add_search_col?(handler, cString)
        }

        index_file?(handler)
        
        window.endSheet(columnSelectionWindow!)
        columnSelectionWindow = nil
        
        // Create and show the main search window
        setupMainSearchWindow()
    }

    private func setupMainSearchWindow() {
        // Create window
        window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1000, height: 600),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )

        if let firstSearchField = searchFields.first {
            window.makeFirstResponder(firstSearchField)
        }
        
        // Create search container
        let searchContainer = NSView(
            frame: NSRect(x: 0, y: 0, width: 1000, height: 50)
        )
        window.contentView?.addSubview(searchContainer)
        searchContainer.translatesAutoresizingMaskIntoConstraints = false
        
        // Add search fields
        let searchColPadding = 20
        let searchColWidth   = CGFloat(1000 / searchBridge.searchColumnNames.count)
        for i in 0..<searchBridge.searchColumnNames.count {
            let searchField = NSSearchField(
                frame: NSRect(x: searchColPadding + i * (searchColPadding + Int(searchColWidth)), y: 10, width: Int(searchColWidth), height: 30)
            )
            searchField.placeholderString = "Search \(searchBridge.searchColumnNames[i])..."
            searchField.sendsSearchStringImmediately = true
            searchContainer.addSubview(searchField)
            searchField.target = self
            searchField.action = #selector(searchFieldChanged(_:))
            searchField.tag = i
            searchFields.append(searchField)

            searchStrings.append("")
        }
        
        // Create scroll view and table
        let scrollView = NSScrollView(
            frame: NSRect(x: 0, y: 0, width: 1000, height: 550)
        )
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        
        tableView = NSTableView(frame: NSRect(x: 0, y: 0, width: 1000, height: 550))
        tableView.dataSource = self
        tableView.delegate = self
        
        // Add columns
        let colWidth = CGFloat(1000 / searchBridge.columnNames.count)
        for i in 0..<searchBridge.columnNames.count {
            let column = NSTableColumn(
                identifier: NSUserInterfaceItemIdentifier("column\(i)")
            )
            column.title = searchBridge.columnNames[i]
            column.width = colWidth;
            tableView.addTableColumn(column)
        }
        
        scrollView.documentView = tableView
        window.contentView?.addSubview(scrollView)
        
        // Set up constraints
        if let contentView = window.contentView {
            NSLayoutConstraint.activate([
                searchContainer.topAnchor.constraint(equalTo: contentView.topAnchor),
                searchContainer.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
                searchContainer.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
                searchContainer.heightAnchor.constraint(equalToConstant: 50)
            ])
            
            scrollView.translatesAutoresizingMaskIntoConstraints = false
            NSLayoutConstraint.activate([
                scrollView.topAnchor.constraint(equalTo: searchContainer.bottomAnchor),
                scrollView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
                scrollView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
                scrollView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor)
            ])
        }
        
        window.title = "Music Search"
        window.center()

        // tableView.backgroundColor = .clear
        // tableView.enclosingScrollView?.drawsBackground = false
        window.appearance = NSAppearance(named: .vibrantDark)

        NSApp.setActivationPolicy(.regular)
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)

        if let firstSearchField = searchFields.first {
            window.makeFirstResponder(firstSearchField)
        }

        ProcessInfo.processInfo.enableSuddenTermination()
    }
    
    @objc func searchFieldChanged(_ sender: NSSearchField) {
        let startTime = Date()

        let column = sender.tag
        let query = sender.stringValue

        searchStrings[column] = query
        
        // Build query string
        var queryParts = searchBridge.searchColumnNames
        for i in 0..<searchBridge.searchColumnNames.count {
            queryParts[i] += "=" + searchStrings[i]
        }
        let queryString = queryParts.joined(separator: "&")
        
        DispatchQueue.global(qos: .userInitiated).async {
            let searchStartTime = Date()
            let results = self.searchBridge.performSearch(query: queryString)
            let searchTime = -searchStartTime.timeIntervalSinceNow
            
            DispatchQueue.main.async {
                let reloadStartTime = Date()
                self.searchResults = results
                self.tableView.reloadData()
                let reloadTime = -reloadStartTime.timeIntervalSinceNow
                
                let totalTime = -startTime.timeIntervalSinceNow

                print("Search Performance Breakdown:")
                print("- Backend Search Time: \(searchTime * 1000) ms")
                print("- Table Reload Time: \(reloadTime * 1000) ms")
                print("- Total Time: \(totalTime * 1000) ms")
            }
        }
    }
    
    func numberOfRows(in tableView: NSTableView) -> Int {
        return searchResults.count
    }
    
    func tableView(_ tableView: NSTableView, objectValueFor tableColumn: NSTableColumn?, row: Int) -> Any? {
        guard let columnIdentifier = tableColumn?.identifier else { return nil }
        let rowData = searchResults[row]
        let columnIndex = Int(columnIdentifier.rawValue.dropFirst(6))!
        return rowData[columnIndex]
    }
    
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        return true
    }
}

let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.run()
