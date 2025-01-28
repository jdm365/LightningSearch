// import Cocoa
import AppKit 
import Foundation
import UniformTypeIdentifiers

let WINDOW_WIDTH  = 1200
let WINDOW_HEIGHT = 900 


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

private let scan_file = dlsym(searchLib, "scanFile")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer
    ) -> Void).self) }
private let index_file = dlsym(searchLib, "indexFile")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer
    ) -> Void).self) }
private let get_indexing_progress = dlsym(searchLib, "getIndexingProgress")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer
    ) -> UInt64).self) }
private let get_num_docs = dlsym(searchLib, "getNumDocs")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer
    ) -> UInt64).self) }

class SearchBridge {
    var queryHandler: UnsafeMutableRawPointer?
    private var resultCount: UInt32 = 0
    private var startPositions = [UInt32](repeating: 0, count: 100 * 3)
    private var lengths = [UInt32](repeating: 0, count: 100 * 3)
    private var resultBuffers = [UnsafeMutablePointer<CChar>?](repeating: nil, count: 100)
    private var numColumns: UInt32 = 0

    var columnNames: [String] = []
    var searchColumnNames: [String] = []
    var searchColMask: [Int] = []
    
    init() {
        if let initFn = init_allocators {
            initFn()
        } else {
            print("Failed to get init_allocators function")
        }

        if let handlerFn = get_handler {
            queryHandler = handlerFn()
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

    func performSearch(query: String) -> [[String]] {
        resultCount = 0
        
        print("Query: \(query)")
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

        var results: [[String]] = []
        for i in 0..<Int(resultCount) {
            var row: [String] = []
            for j in 0..<Int(numColumns) {
            // for j in searchColMask {

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
        
        return results
    }
}

class AppDelegate: NSObject, NSApplicationDelegate, NSTableViewDataSource, NSTableViewDelegate {
   var window: NSWindow!
   var tableView: NSTableView!
   var searchFields: [NSSearchField] = []
   var searchResults: [[String]] = []
   var searchStrings: [String] = []
   var searchBridge: SearchBridge!
   var columnSelectionWindow: NSWindow!
   var checkboxes: [NSButton] = []
   var searchContainer: NSView!
   var scrollView: NSScrollView!
   var fileSelectionView: NSView!

   // Side panel data
   var splitView: NSSplitView!
   var detailsView: NSScrollView!
   var detailsTable: NSTableView!
   var selectedRowData: [String: String] = [:]

   private var openPanel: NSOpenPanel!
   private let searchQueue = DispatchQueue(label: "com.search.queue")
   
   func applicationDidFinishLaunching(_ notification: Notification) {
       window = NSWindow(
           contentRect: NSRect(x: 0, y: 0, width: WINDOW_WIDTH, height: WINDOW_HEIGHT),
           // styleMask: [.titled, .closable, .miniaturizable, .resizable, .fullSizeContentView],
           styleMask: [.titled, .closable, .miniaturizable, .resizable],
           backing: .buffered,
           defer: false
       )
       
        // window.isOpaque = false
        // window.backgroundColor = NSColor.clear
        window.appearance = NSAppearance(named: .vibrantDark)

        // // Add the background image
        // let backgroundImageView = NSImageView(frame: window.contentView!.bounds)
        // backgroundImageView.image = NSImage(named: "../../logo.png") // Replace with your image name
        // backgroundImageView.imageScaling = .scaleAxesIndependently
        // backgroundImageView.alphaValue = 0.8 // Adjust transparency (0.0 = fully transparent, 1.0 = fully opaque)
        // backgroundImageView.autoresizingMask = [.width, .height] // Ensure it resizes with the window
        // window.contentView?.addSubview(backgroundImageView, positioned: .below, relativeTo: nil)
       
       fileSelectionView = NSView(frame: window.contentView!.bounds)
       window.contentView?.addSubview(fileSelectionView)
       fileSelectionView.translatesAutoresizingMaskIntoConstraints = false
       
       let label = NSTextField(frame: .zero)
       label.stringValue = "Please select a CSV or JSON file to index"
       label.isEditable = false
       label.isBordered = false
       label.drawsBackground = false
       label.alignment = .center
       label.font = .systemFont(ofSize: 16)
       label.translatesAutoresizingMaskIntoConstraints = false
       fileSelectionView.addSubview(label)
       
       let button = NSButton(frame: .zero)
       button.title = "Choose File"
       button.bezelStyle = .rounded
       button.target = self
       button.action = #selector(chooseFile)
       button.translatesAutoresizingMaskIntoConstraints = false
       fileSelectionView.addSubview(button)
       
       NSLayoutConstraint.activate([
           fileSelectionView.topAnchor.constraint(equalTo: window.contentView!.topAnchor),
           fileSelectionView.leadingAnchor.constraint(equalTo: window.contentView!.leadingAnchor),
           fileSelectionView.trailingAnchor.constraint(equalTo: window.contentView!.trailingAnchor),
           fileSelectionView.bottomAnchor.constraint(equalTo: window.contentView!.bottomAnchor),
           
           label.centerXAnchor.constraint(equalTo: fileSelectionView.centerXAnchor),
           label.bottomAnchor.constraint(equalTo: fileSelectionView.centerYAnchor, constant: -20),
           
           button.centerXAnchor.constraint(equalTo: fileSelectionView.centerXAnchor),
           button.topAnchor.constraint(equalTo: label.bottomAnchor, constant: 20)
       ])
       
       window.title = "Search"
       window.center()

       NSApp.setActivationPolicy(.regular)
       NSApp.activate(ignoringOtherApps: true)
       window.makeKeyAndOrderFront(nil)
       window.level = .floating

       DispatchQueue.main.async {
            self.searchBridge = SearchBridge()
            self.openPanel = NSOpenPanel()
            self.openPanel.allowsMultipleSelection = false
            self.openPanel.canChooseDirectories = false
            self.openPanel.canChooseFiles = true
            self.openPanel.allowedContentTypes = [UTType.commaSeparatedText, UTType.json]
       }
   }

   @objc func chooseFile() {
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

        let group = DispatchGroup()
        
        // Launch scan operation
        group.enter()
        DispatchQueue.global(qos: .userInitiated).async {
            scan_file?(handler)
            group.leave()
        }
        
        // Launch UI update
        group.enter()
        DispatchQueue.main.async {
            self.showColumnSelection()
            group.leave()
        }
        
        // Wait for both to complete
        group.notify(queue: .main) {
            // Both operations are now complete
            // Put any completion handling code here
        }
   }

   private func showColumnSelection() {
       columnSelectionWindow = NSWindow(
           contentRect: NSRect(x: 0, y: 0, width: 300, height: CGFloat(50 + searchBridge.columnNames.count * 30)),
           styleMask: [.titled, .closable],
           backing: .buffered,
           defer: false
       )
       
       let contentView = NSView(frame: columnSelectionWindow.contentRect(forFrameRect: columnSelectionWindow.frame))
       columnSelectionWindow.contentView = contentView
       
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
       
       columnSelectionWindow.title = "Select Search Columns"
       columnSelectionWindow.center()
       window.beginSheet(columnSelectionWindow) { response in }
   }

   @objc func startIndexing() {
       guard let handler = searchBridge.queryHandler else {
           print("QueryHandler is nil")
           return
       }

       searchBridge.searchColumnNames = checkboxes.enumerated()
           .filter { $0.element.state == .on }
           .map { searchBridge.columnNames[$0.offset] }
       searchBridge.searchColMask = checkboxes.enumerated()
           .filter { $0.element.state == .on }
           .map { $0.offset }
       
       if searchBridge.searchColumnNames.isEmpty {
           let alert = NSAlert()
           alert.messageText = "Please select at least one column"
           alert.alertStyle = .warning
           alert.beginSheetModal(for: columnSelectionWindow) { _ in }
           return
       }

       window.endSheet(columnSelectionWindow)
       columnSelectionWindow = nil

       for subview in fileSelectionView.subviews {
           subview.isHidden = true
       }

        let totalDocs = Double(get_num_docs!(handler) / 1000)

        // Create progress bar
        let progressBar = NSProgressIndicator()
        progressBar.style = .bar
        progressBar.isIndeterminate = false
        progressBar.minValue = 0
        progressBar.maxValue = totalDocs
        progressBar.translatesAutoresizingMaskIntoConstraints = false
        
        // Create progress label
        let progressLabel = NSTextField()
        progressLabel.isEditable = false
        progressLabel.isBordered = false
        progressLabel.drawsBackground = false
        progressLabel.stringValue = "Processed 0K documents"
        progressLabel.font = .systemFont(ofSize: 14)
        progressLabel.alignment = .center
        progressLabel.translatesAutoresizingMaskIntoConstraints = false
        
        window.contentView?.addSubview(progressBar)
        window.contentView?.addSubview(progressLabel)
        
        // Set up constraints
        NSLayoutConstraint.activate([
            progressBar.centerXAnchor.constraint(equalTo: window.contentView!.centerXAnchor),
            progressBar.centerYAnchor.constraint(equalTo: window.contentView!.centerYAnchor),
            progressBar.widthAnchor.constraint(equalToConstant: 300),
            
            progressLabel.centerXAnchor.constraint(equalTo: window.contentView!.centerXAnchor),
            progressLabel.topAnchor.constraint(equalTo: progressBar.bottomAnchor, constant: 10)
        ])

        // Create a timer to update progress
        var progressTimer: Timer?
        let totalDocsDigits = String(Int(totalDocs/1000)).count
        progressTimer = Timer.scheduledTimer(withTimeInterval: 1.0/30.0, repeats: true) { [weak self] timer in
            guard let _ = self else {
                timer.invalidate()
                return
            }
            
            if let progress = get_indexing_progress?(handler) {
                DispatchQueue.main.async {
                    // Update progress bar
                    progressBar.doubleValue = Double(progress) / 1000

                    // Use string format with width specification
                    let text = String(format: "Processed %\(totalDocsDigits)d / %dK documents", 
                        Int(progress/1000), 
                        Int(totalDocs))

                    progressLabel.attributedStringValue = NSAttributedString(
                        string: text,
                        attributes: [.font: NSFont.monospacedSystemFont(ofSize: 14, weight: .regular)]
    )
                }
            }
        }


       // Do the indexing work asynchronously
       DispatchQueue.global(qos: .userInitiated).async {
           // Add columns and index in background
           for i in 0..<self.searchBridge.searchColumnNames.count {
               guard let cString = self.searchBridge.searchColumnNames[i].cString(using: .utf8) else {
                   print("Failed to convert column name to C string")
                   continue
               }
               add_search_col?(handler, cString)
           }

           index_file?(handler)
           
           // Set up UI on main thread
           DispatchQueue.main.async {
                // label.removeFromSuperview()
                progressTimer?.invalidate()
                progressTimer = nil
                progressBar.removeFromSuperview()
                progressLabel.removeFromSuperview()
                self.setupSearchInterface()
           }
       }
   }

    private func setupSearchInterface() {
        fileSelectionView.removeFromSuperview()

        // Create split view to hold table and details
        splitView = NSSplitView()
        splitView.isVertical = true
        splitView.dividerStyle = .thin
        splitView.autoresizingMask = [.width]
        window.contentView?.addSubview(splitView)
        splitView.translatesAutoresizingMaskIntoConstraints = false

        // Create container for search and table
        let tableContainer = NSView()
        tableContainer.translatesAutoresizingMaskIntoConstraints = false
        splitView.addArrangedSubview(tableContainer)

        // Add search container to table container
        searchContainer = NSView()
        tableContainer.addSubview(searchContainer)
        searchContainer.translatesAutoresizingMaskIntoConstraints = false

        tableView = NSTableView()
        tableView.dataSource = self
        tableView.delegate = self
        tableView.target = self
        tableView.action = #selector(tableViewClicked(_:))
        tableView.columnAutoresizingStyle = .uniformColumnAutoresizingStyle

        let searchColPadding = CGFloat(20)

        NSLayoutConstraint.activate([
            splitView.topAnchor.constraint(equalTo: window.contentView!.topAnchor),
            splitView.leadingAnchor.constraint(equalTo: window.contentView!.leadingAnchor),
            splitView.trailingAnchor.constraint(equalTo: window.contentView!.trailingAnchor),
            splitView.bottomAnchor.constraint(equalTo: window.contentView!.bottomAnchor)
        ])
        window.layoutIfNeeded()
        print("splitView frame width: \(tableContainer.frame.width)")

        // Create details view
        detailsView = NSScrollView()
        detailsView.hasVerticalScroller = true
        detailsTable = NSTableView()
        detailsTable.addTableColumn(
            NSTableColumn(identifier: NSUserInterfaceItemIdentifier("key"))
            )
        detailsTable.addTableColumn(
            NSTableColumn(identifier: NSUserInterfaceItemIdentifier("value"))
            )
        detailsTable.headerView = nil
        detailsTable.dataSource = self
        detailsTable.delegate = self
        detailsView.documentView = detailsTable
        detailsView.contentInsets = NSEdgeInsets(
            top: 10, 
            left: 0, 
            bottom: 10, 
            right: 0
            )
        splitView.addArrangedSubview(tableContainer)
        splitView.addArrangedSubview(detailsView)
       
        // Set the initial division
        splitView.setPosition(window.frame.width * 0.7, ofDividerAt: 0)
        window.layoutIfNeeded()

        // Then set up tableContainer constraints
        NSLayoutConstraint.activate([
            tableContainer.widthAnchor.constraint(equalTo: splitView.widthAnchor, multiplier: 0.7),
            tableContainer.heightAnchor.constraint(equalTo: splitView.heightAnchor),
            tableContainer.topAnchor.constraint(equalTo: splitView.topAnchor),
            tableContainer.leadingAnchor.constraint(equalTo: splitView.leadingAnchor)
        ])
        window.layoutIfNeeded()
        print("Table Container frame width: \(tableContainer.frame.width)")

        NSLayoutConstraint.activate([
            searchContainer.widthAnchor.constraint(equalTo: tableContainer.widthAnchor),
            searchContainer.topAnchor.constraint(equalTo: tableContainer.topAnchor),
            searchContainer.leadingAnchor.constraint(equalTo: tableContainer.leadingAnchor),
            searchContainer.trailingAnchor.constraint(equalTo: tableContainer.trailingAnchor),
            searchContainer.heightAnchor.constraint(equalToConstant: 50)
        ])
        window.layoutIfNeeded()
        print("Search Container frame width: \(searchContainer.frame.width)")

        let numCols = searchBridge.searchColumnNames.count
        let searchWidth = searchContainer.frame.width
        let columnWidth = (searchWidth / CGFloat(numCols)) - searchColPadding * 2
        for i in 0..<searchBridge.searchColumnNames.count {
            let searchField = NSSearchField()
            searchField.placeholderString = "Search \(searchBridge.searchColumnNames[i])..."
            searchField.sendsSearchStringImmediately = true
            searchContainer.addSubview(searchField)
            searchField.target = self
            searchField.action = #selector(searchFieldChanged(_:))
            searchField.tag = i
            searchField.translatesAutoresizingMaskIntoConstraints = false

            let xPosition = searchColPadding + 
                            (CGFloat(i) * (columnWidth + searchColPadding * 2))
            NSLayoutConstraint.activate([
                searchField.leadingAnchor.constraint(
                    equalTo: searchContainer.leadingAnchor, 
                    constant: xPosition
                ),
                searchField.centerYAnchor.constraint(
                    equalTo: searchContainer.centerYAnchor
                ),
                searchField.widthAnchor.constraint(equalToConstant: columnWidth),
                searchField.heightAnchor.constraint(equalToConstant: 30)
            ])
           
            searchFields.append(searchField)
            searchStrings.append("")
       }

        scrollView = NSScrollView()
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        tableContainer.addSubview(scrollView)
        scrollView.translatesAutoresizingMaskIntoConstraints = false
       
        for i in 0..<searchBridge.searchColumnNames.count {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("column\(i)"))
            column.title = searchBridge.searchColumnNames[i]
            column.width = columnWidth 
            column.resizingMask = [.userResizingMask, .autoresizingMask]
            tableView.addTableColumn(column)
        }
       
        scrollView.documentView = tableView

        // Set up constraints
        if let _ = window.contentView {
            NSLayoutConstraint.activate([
                scrollView.topAnchor.constraint(equalTo: searchContainer.bottomAnchor),
                scrollView.leadingAnchor.constraint(equalTo: tableContainer.leadingAnchor),
                scrollView.trailingAnchor.constraint(equalTo: tableContainer.trailingAnchor),
                scrollView.bottomAnchor.constraint(equalTo: tableContainer.bottomAnchor)
            ])
        }

        // NotificationCenter.default.addObserver(
            // forName: NSView.frameDidChangeNotification, 
            // object: splitView, 
            // queue: nil
            // ) { [weak self] _ in
            // guard let self = self else { return }
            // let numCols = self.searchBridge.searchColumnNames.count
            // let searchWidth = searchContainer.frame.width
            // let columnWidth = (searchWidth / CGFloat(numCols)) - searchColPadding * 2
           //  
            // for (i, searchField) in self.searchFields.enumerated() {
                // let xPosition = searchColPadding + (CGFloat(i) * (columnWidth + searchColPadding * 2))
                // if let constraint = searchField.constraints.first(where: { $0.firstAttribute == .leading }) {
                    // constraint.constant = xPosition
                // }
                // if let constraint = searchField.constraints.first(where: { $0.firstAttribute == .width }) {
                    // constraint.constant = columnWidth
                // }
            // }
        // }
    
        // Set initial split view proportions
        // splitView.setPosition(window.frame.width * 0.7, ofDividerAt: 0)
       
       if let firstSearchField = searchFields.first {
           window.makeFirstResponder(firstSearchField)
       }

        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
   }

    // Add data source methods for details table
    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView == self.tableView {
            return searchResults.count
        } else if tableView == detailsTable {
            return selectedRowData.count
        }
        return 0
    }

    // Add row selection handler
    @objc func tableViewClicked(_ sender: NSTableView) {
        if (sender.selectedRow < 0) || (sender.selectedRow >= searchResults.count) {
            return
        }
        let rowData = searchResults[sender.selectedRow]
        selectedRowData.removeAll()
        
        // Map column names to values
        for (index, name) in searchBridge.columnNames.enumerated() {
            selectedRowData[name] = rowData[index]
        }
        
        detailsTable.reloadData()
    }
   
   @objc func searchFieldChanged(_ sender: NSSearchField) {
       let startTime = Date()

       let column = sender.tag
       let query  = sender.stringValue

       searchStrings[column] = query
       
       var queryParts = searchBridge.searchColumnNames
       for i in 0..<searchBridge.searchColumnNames.count {
           queryParts[i] += "=" + searchStrings[i]
       }
       let queryString = queryParts.joined(separator: "&")
       
       // DispatchQueue.global(qos: .userInitiated).async {
       searchQueue.async { [weak self] in
            guard let self = self else { return }

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
        if searchResults.count == 0 {
            return
        }

        // Click first result.
        tableView.selectRowIndexes(
            IndexSet(integer: 0), 
            byExtendingSelection: false
            )

        // Call the click handler manually
        tableViewClicked(tableView)
   }
   
   func tableView(_ tableView: NSTableView, objectValueFor tableColumn: NSTableColumn?, row: Int) -> Any? {
       if tableView == self.tableView {

            guard let columnIdentifier = tableColumn?.identifier else { return nil }
            let rowData = searchResults[row]

            // Drop 6 charachters column in columnX string.
            let columnIndex = searchBridge.searchColMask[Int(columnIdentifier.rawValue.dropFirst(6))!]
            return rowData[columnIndex]
       } else if tableView == detailsTable {

            let sortedKeys = selectedRowData.keys.sorted()
            if tableColumn?.identifier.rawValue == "key" {
                return sortedKeys[row]
            } else {
                return selectedRowData[sortedKeys[row]]
            }
       }
       return nil
   }

   func applicationWillTerminate(_ notification: Notification) {
       NSApp.setActivationPolicy(.prohibited)
       NSApp.hide(nil)
       DispatchQueue.main.async {
           exit(0)
       }
   }

   func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
       NSApp.hide(nil)
       return .terminateNow
    }
   
   func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
       return true;
   }
}


let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.run()
