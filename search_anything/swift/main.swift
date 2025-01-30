import AppKit 
import Foundation
import UniformTypeIdentifiers

let screenFrame = NSScreen.main?.frame
let screenWidth = screenFrame?.width
let screenHeight = screenFrame?.height

let WINDOW_WIDTH  = screenWidth! * 0.8
let WINDOW_HEIGHT = screenHeight! * 0.85


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


class SmoothProgressBar: NSView {
    private var progress: Double = 0.0
    
    private let progressLayer = CALayer()

    var minValue: Double = 0.0
    var maxValue: Double = 1.0
    
    override init(frame: NSRect) {
        super.init(frame: frame)
        setupView()
    }
    
    required init?(coder: NSCoder) {
        super.init(coder: coder)
        setupView()
    }
    
    private func setupView() {
        wantsLayer = true
        layer?.backgroundColor = NSColor(white: 0.3, alpha: 0.3).cgColor
        layer?.cornerRadius = 0.75
        
        progressLayer.backgroundColor = NSColor.systemBlue.cgColor
        progressLayer.frame = CGRect(x: 0, y: 0, width: 0, height: bounds.height)
        progressLayer.cornerRadius = 0.75
        
        layer?.addSublayer(progressLayer)
    }
    
    override func layout() {
        super.layout()
        progressLayer.frame = CGRect(x: 0, y: 0, width: bounds.width * CGFloat(progress), height: bounds.height)
    }
    
    func setProgress(_ newProgress: Double, animated: Bool = true) {
        progress = newProgress / (maxValue - minValue)
        progress = max(0.0, min(1.0, progress))
        
        if animated {
            CATransaction.begin()
            CATransaction.setAnimationDuration(0.2)
            progressLayer.frame = CGRect(x: 0, y: 0, width: bounds.width * CGFloat(progress), height: bounds.height)
            CATransaction.commit()
        } else {
            progressLayer.frame = CGRect(x: 0, y: 0, width: bounds.width * CGFloat(progress), height: bounds.height)
        }
    }
}

class AppDelegate: NSObject, NSApplicationDelegate, NSTableViewDataSource, NSTableViewDelegate {
    var mainWindow: NSWindow!
    var backgroundImageView: NSImageView!

    // File selection data
    var fileSelectionView: NSView!
    var checkboxes: [NSButton] = []
    var openPanel: NSOpenPanel!

    // Search data
    var searchFields: [NSSearchField] = []
    var searchResults: [[String]] = []
    var searchStrings: [String] = []
    var searchBridge: SearchBridge!
    let searchQueue = DispatchQueue(label: "com.search.queue")

    // Search interface data
    var tableView: NSTableView!
    var columnSelectionWindow: NSWindow!
    var searchContainer: NSView!

    // Side panel data
    var splitView: NSSplitView!
    var detailsView: NSScrollView!
    var detailsTable: NSTableView!
    var selectedRowData: [String: String] = [:]
    var scrollView: NSScrollView!

   
    func applicationDidFinishLaunching(_ notification: Notification) {
        /*******************************************************
        -----------------
             LAYOUT 
        -----------------

        MAIN WINDOW
            FILE SELECTION VIEW
                SELECT FILE BUTTON

            SPLIT VIEW
                SEARCH CONTAINER
                    SEARCH FIELDS
                TABLE VIEW
                DETAILS VIEW
                    DETAILS TABLE
        ********************************************************/

        setupMainWindow()
        setupBackgroundImage()
        setupFileSelectionView()

        DispatchQueue.main.async {
            self.searchBridge = SearchBridge()
            self.openPanel = NSOpenPanel()
            self.openPanel.allowsMultipleSelection = false
            self.openPanel.canChooseDirectories = false
            self.openPanel.canChooseFiles = true
            self.openPanel.allowedContentTypes = [
                UTType.commaSeparatedText, 
                UTType.json
                ]
        }
   }

    private func setupMainWindow() {
        mainWindow = NSWindow(
            contentRect: NSRect(
                x: 0, 
                y: 0, 
                width: WINDOW_WIDTH, 
                height: WINDOW_HEIGHT
            ),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )

        mainWindow.appearance = NSAppearance(named: .vibrantDark)
        mainWindow.title = "Search"
        mainWindow.center()

        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
        mainWindow.makeKeyAndOrderFront(nil)
        mainWindow.level = .floating
    }

    // private func setupBackgroundImage() {
        // // Create background image view that fills the mainWindow
        // backgroundImageView = NSImageView(frame: mainWindow.contentView!.bounds)
        // if let image = NSImage(contentsOfFile: "logo.png") {
            // backgroundImageView.image = image
            // backgroundImageView.imageScaling = .scaleProportionallyUpOrDown
            // backgroundImageView.alphaValue = 0.65
            // backgroundImageView.autoresizingMask = [.width, .height]
// 
            // // Add the background behind all other content
            // mainWindow.contentView?.addSubview(
                // backgroundImageView, 
                // positioned: .below, 
                // relativeTo: nil
                // )
        // }
    // }

    private func setupBackgroundImage() {
        backgroundImageView = NSImageView(frame: mainWindow.contentView!.bounds)
        if let image = NSImage(contentsOfFile: "logo.png") {
            // Convert NSImage to CIImage for filtering
            guard let cgImage = image.cgImage(forProposedRect: nil, context: nil, hints: nil) else { return }
            let ciImage = CIImage(cgImage: cgImage)
            
            // Create blur filter
            let filter = CIFilter(name: "CIGaussianBlur")!
            filter.setValue(ciImage, forKey: kCIInputImageKey)
            filter.setValue(3.0, forKey: kCIInputRadiusKey)
            
            // Get filtered image
            if let outputCIImage = filter.outputImage {
                let context = CIContext()
                if let outputCGImage = context.createCGImage(outputCIImage, from: outputCIImage.extent) {
                    let blurredImage = NSImage(cgImage: outputCGImage, size: image.size)
                    backgroundImageView.image = blurredImage
                }
            }
            
            backgroundImageView.imageScaling = .scaleProportionallyUpOrDown
            backgroundImageView.autoresizingMask = [.width, .height]
            
            mainWindow.contentView?.addSubview(
                backgroundImageView, 
                positioned: .below, 
                relativeTo: nil
            )
        }
    }

    private func setupFileSelectionView() {
        fileSelectionView = NSView(frame: mainWindow.contentView!.bounds)
        mainWindow.contentView?.addSubview(fileSelectionView)
        fileSelectionView.translatesAutoresizingMaskIntoConstraints = false
       
        let button = NSButton(frame: .zero)
        button.target = self
        button.title = "Select File"
        button.alignment = .center
        button.bezelStyle = .rounded
        button.action = #selector(chooseFile)
        button.translatesAutoresizingMaskIntoConstraints = false

        fileSelectionView.addSubview(button)
       
        NSLayoutConstraint.activate([
            fileSelectionView.topAnchor.constraint(equalTo: mainWindow.contentView!.topAnchor),
            fileSelectionView.leadingAnchor.constraint(equalTo: mainWindow.contentView!.leadingAnchor),
            fileSelectionView.trailingAnchor.constraint(equalTo: mainWindow.contentView!.trailingAnchor),
            fileSelectionView.bottomAnchor.constraint(equalTo: mainWindow.contentView!.bottomAnchor),
           
            button.centerXAnchor.constraint(equalTo: fileSelectionView.centerXAnchor),
            button.bottomAnchor.constraint(equalTo: fileSelectionView.centerYAnchor, constant: 30),
        ])
    }

    @objc func chooseFile() {
        openPanel.beginSheetModal(for: mainWindow) { [weak self] response in
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
        group.notify(queue: .main) {}
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
            let checkbox = NSButton(
                frame: NSRect(
                    x: 20, 
                    y: CGFloat((searchBridge.columnNames.count - 1 - index) * 30 + 20), 
                    width: 200, 
                    height: 20
                    )
                )
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
        startButton.keyEquivalent = "\r"
        contentView.addSubview(startButton)
       
        columnSelectionWindow.title = "Select Search Columns"
        columnSelectionWindow.center()
        mainWindow.beginSheet(columnSelectionWindow) { response in }
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

        mainWindow.endSheet(columnSelectionWindow)
        columnSelectionWindow = nil

        for subview in fileSelectionView.subviews {
            subview.isHidden = true
        }

        let totalDocs = Double(get_num_docs!(handler) / 1000)

        // Create progress bar
        let progressBar = SmoothProgressBar(frame: NSRect(x: 0, y: 0, width: 400, height: 10))
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
        
        mainWindow.contentView?.addSubview(progressBar)
        mainWindow.contentView?.addSubview(progressLabel)
        
        // Set up constraints
        NSLayoutConstraint.activate([
            progressBar.centerXAnchor.constraint(equalTo: mainWindow.contentView!.centerXAnchor),
            progressBar.centerYAnchor.constraint(equalTo: mainWindow.contentView!.centerYAnchor),
            progressBar.widthAnchor.constraint(equalToConstant: progressBar.frame.width),
            progressBar.heightAnchor.constraint(equalToConstant: progressBar.frame.height),
            
            progressLabel.centerXAnchor.constraint(equalTo: mainWindow.contentView!.centerXAnchor),
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
                    // progressBar.doubleValue = Double(progress) / 1000
                    progressBar.setProgress(Double(progress) / 1000)

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

        // Ensure background image stays behind new content
        if let backgroundView = backgroundImageView {
            mainWindow.contentView?.addSubview(
                backgroundView, 
                positioned: .below, 
                relativeTo: nil
                )
        }

        // Create split view to hold table and details
        splitView = NSSplitView()
        splitView.isVertical = true
        // splitView.dividerStyle = .thin
        splitView.dividerStyle = .paneSplitter
        splitView.autoresizingMask = [.width]
        splitView.layer?.backgroundColor = .clear
        splitView.translatesAutoresizingMaskIntoConstraints = false

        mainWindow.contentView?.addSubview(splitView)

        // Create container for search and table
        let tableContainer = NSView()
        tableContainer.translatesAutoresizingMaskIntoConstraints = false

        splitView.addArrangedSubview(tableContainer)

        // Add search container to table container
        searchContainer = NSView()
        searchContainer.wantsLayer = true
        searchContainer.layer?.backgroundColor = NSColor(
            white: 0.1, 
            alpha: 0.4
            ).cgColor
        searchContainer.translatesAutoresizingMaskIntoConstraints = false

        tableContainer.addSubview(searchContainer)

        tableView = NSTableView()
        tableView.dataSource = self
        tableView.delegate = self
        tableView.target = self
        tableView.action = #selector(tableViewClicked(_:))
        tableView.columnAutoresizingStyle = .uniformColumnAutoresizingStyle

        // Make table view transparent
        tableView.backgroundColor = .clear
        tableView.gridColor = .clear
        tableView.gridStyleMask = .solidHorizontalGridLineMask
        tableView.intercellSpacing = NSSize(width: 0, height: 0) 

        let appearance = NSAppearance(named: .vibrantDark)
        tableView.appearance = appearance

        tableView.rowSizeStyle = .default
        tableView.selectionHighlightStyle = .regular

        let searchColPadding = CGFloat(20)
        NSLayoutConstraint.activate([
            splitView.topAnchor.constraint(equalTo: mainWindow.contentView!.topAnchor),
            splitView.leadingAnchor.constraint(equalTo: mainWindow.contentView!.leadingAnchor),
            splitView.trailingAnchor.constraint(equalTo: mainWindow.contentView!.trailingAnchor),
            splitView.bottomAnchor.constraint(equalTo: mainWindow.contentView!.bottomAnchor)
        ])
        mainWindow.layoutIfNeeded()

        // Create details view
        detailsView = NSScrollView()
        detailsView.hasVerticalScroller = true
        detailsView.drawsBackground = false

        detailsTable = NSTableView()
        detailsTable.backgroundColor = .clear
        detailsTable.gridColor = .clear

        let keyColumn = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("key"))
        let valueColumn = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("value"))
        keyColumn.title = "Key"
        valueColumn.title = "Value"
        keyColumn.headerCell.font = .systemFont(ofSize: 14)
        valueColumn.headerCell.font = .systemFont(ofSize: 14)
        detailsTable.addTableColumn(keyColumn)
        detailsTable.addTableColumn(valueColumn)

        // Add header to details table
        let header = NSTableHeaderView()
        header.frame = NSRect(x: 0, y: 0, width: 200, height: 30)
        header.layer?.backgroundColor = .clear

        detailsTable.headerView = header

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
        splitView.setPosition(mainWindow.frame.width * 0.7, ofDividerAt: 0)
        mainWindow.layoutIfNeeded()

        // Then set up tableContainer constraints
        NSLayoutConstraint.activate([
            tableContainer.widthAnchor.constraint(equalTo: splitView.widthAnchor, multiplier: 0.7),
            tableContainer.heightAnchor.constraint(equalTo: splitView.heightAnchor),
            tableContainer.topAnchor.constraint(equalTo: splitView.topAnchor),
            tableContainer.leadingAnchor.constraint(equalTo: splitView.leadingAnchor)
        ])
        mainWindow.layoutIfNeeded()

        NSLayoutConstraint.activate([
            searchContainer.widthAnchor.constraint(equalTo: tableContainer.widthAnchor),
            searchContainer.topAnchor.constraint(equalTo: tableContainer.topAnchor),
            searchContainer.leadingAnchor.constraint(equalTo: tableContainer.leadingAnchor),
            searchContainer.trailingAnchor.constraint(equalTo: tableContainer.trailingAnchor),
            searchContainer.heightAnchor.constraint(equalToConstant: 50)
        ])
        mainWindow.layoutIfNeeded()

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


            // Add this when creating each search field
            searchField.nextKeyView = nil
       }

        for i in 0..<searchFields.count {
            let currentField = searchFields[i]
            let nextField = searchFields[(i + 1) % searchFields.count]
            currentField.nextKeyView = nextField
        }


        scrollView = NSScrollView()
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.drawsBackground = false
        scrollView.backgroundColor = .clear
        tableContainer.addSubview(scrollView)
        scrollView.translatesAutoresizingMaskIntoConstraints = false
       
        for i in 0..<searchBridge.searchColumnNames.count {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("column\(i)"))
            column.title = searchBridge.searchColumnNames[i]
            column.width = columnWidth 
            column.resizingMask = [.userResizingMask, .autoresizingMask]

            // Style the header
            let headerCell = column.headerCell
            headerCell.textColor = .white
            column.headerCell = headerCell

            tableView.addTableColumn(column)
        }
       
        scrollView.documentView = tableView

        // Set up constraints
        if let _ = mainWindow.contentView {
            NSLayoutConstraint.activate([
                scrollView.topAnchor.constraint(equalTo: searchContainer.bottomAnchor),
                scrollView.leadingAnchor.constraint(equalTo: tableContainer.leadingAnchor),
                scrollView.trailingAnchor.constraint(equalTo: tableContainer.trailingAnchor),
                scrollView.bottomAnchor.constraint(equalTo: tableContainer.bottomAnchor)
            ])
        }

       if let firstSearchField = searchFields.first {
           mainWindow.makeFirstResponder(firstSearchField)
       }

        mainWindow.makeKeyAndOrderFront(nil)
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

    func tableView(_ tableView: NSTableView, willDisplayCell cell: Any, for tableColumn: NSTableColumn?, row: Int) {
        if tableView == self.tableView {
            if let textCell = cell as? NSTextFieldCell {
                // Set the background color based on row number
                let backgroundColor = row % 2 == 0 ? 
                    NSColor(white: 0.2, alpha: 0.6) :  // Even rows - very subtle
                    NSColor(white: 0.1, alpha: 0.6)    // Odd rows - slightly more visible
                
                textCell.backgroundColor = backgroundColor
                textCell.drawsBackground = true
            }
        }
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
