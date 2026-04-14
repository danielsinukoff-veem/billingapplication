import Foundation
import PDFKit

struct ContractText: Codable {
    let path: String
    let name: String
    let pageCount: Int
    let text: String
}

enum ExtractionError: Error, CustomStringConvertible {
    case invalidArguments
    case unreadableDirectory(String)
    case encodeFailure

    var description: String {
        switch self {
        case .invalidArguments:
            return "Usage: swift extract_pdf_text.swift <pdf-folder>"
        case .unreadableDirectory(let path):
            return "Could not read PDF folder: \(path)"
        case .encodeFailure:
            return "Could not encode extraction output as JSON"
        }
    }
}

func listPDFs(in folder: URL) throws -> [URL] {
    let manager = FileManager.default
    guard let enumerator = manager.enumerator(at: folder, includingPropertiesForKeys: [.isRegularFileKey], options: [.skipsHiddenFiles]) else {
        throw ExtractionError.unreadableDirectory(folder.path)
    }
    var pdfs: [URL] = []
    for case let fileURL as URL in enumerator {
        guard fileURL.pathExtension.lowercased() == "pdf" else { continue }
        let values = try fileURL.resourceValues(forKeys: [.isRegularFileKey])
        if values.isRegularFile == true {
            pdfs.append(fileURL)
        }
    }
    return pdfs.sorted { $0.lastPathComponent.localizedCaseInsensitiveCompare($1.lastPathComponent) == .orderedAscending }
}

func extractText(from pdfURL: URL) -> ContractText {
    if let document = PDFDocument(url: pdfURL) {
        return ContractText(
            path: pdfURL.path,
            name: pdfURL.lastPathComponent,
            pageCount: document.pageCount,
            text: document.string ?? ""
        )
    }

    return ContractText(
        path: pdfURL.path,
        name: pdfURL.lastPathComponent,
        pageCount: 0,
        text: ""
    )
}

do {
    guard CommandLine.arguments.count == 2 else {
        throw ExtractionError.invalidArguments
    }

    let folder = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
    let pdfs = try listPDFs(in: folder)
    let documents = pdfs.map(extractText)

    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    guard let data = try? encoder.encode(documents) else {
        throw ExtractionError.encodeFailure
    }
    FileHandle.standardOutput.write(data)
} catch {
    FileHandle.standardError.write(Data((error.localizedDescription + "\n").utf8))
    exit(1)
}
