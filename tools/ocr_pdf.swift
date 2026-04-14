import AppKit
import Foundation
import PDFKit
import Vision

enum OCRScriptError: Error, CustomStringConvertible {
    case invalidArguments
    case unreadablePDF(String)

    var description: String {
        switch self {
        case .invalidArguments:
            return "Usage: swift ocr_pdf.swift <pdf-path>"
        case .unreadablePDF(let path):
            return "Could not open PDF at \(path)"
        }
    }
}

func renderPage(_ page: PDFPage, scale: CGFloat = 2.0) -> CGImage? {
    let bounds = page.bounds(for: .mediaBox)
    let width = max(Int(bounds.width * scale), 1)
    let height = max(Int(bounds.height * scale), 1)
    guard let context = CGContext(
        data: nil,
        width: width,
        height: height,
        bitsPerComponent: 8,
        bytesPerRow: 0,
        space: CGColorSpaceCreateDeviceRGB(),
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
    ) else {
        return nil
    }

    context.setFillColor(NSColor.white.cgColor)
    context.fill(CGRect(x: 0, y: 0, width: CGFloat(width), height: CGFloat(height)))
    context.scaleBy(x: scale, y: scale)
    page.draw(with: .mediaBox, to: context)
    return context.makeImage()
}

func recognizeText(from image: CGImage) -> String {
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true

    let handler = VNImageRequestHandler(cgImage: image, options: [:])
    do {
        try handler.perform([request])
    } catch {
        return ""
    }

    guard let observations = request.results else {
        return ""
    }

    return observations.compactMap { observation in
        observation.topCandidates(1).first?.string
    }.joined(separator: "\n")
}

do {
    guard CommandLine.arguments.count == 2 else {
        throw OCRScriptError.invalidArguments
    }

    let pdfPath = CommandLine.arguments[1]
    let url = URL(fileURLWithPath: pdfPath)
    guard let document = PDFDocument(url: url) else {
        throw OCRScriptError.unreadablePDF(pdfPath)
    }

    var pages: [String] = []
    for index in 0..<document.pageCount {
        guard let page = document.page(at: index), let image = renderPage(page) else {
            continue
        }
        let text = recognizeText(from: image)
        pages.append("=== Page \(index + 1) ===\n" + text)
    }

    FileHandle.standardOutput.write(Data(pages.joined(separator: "\n\n").utf8))
} catch {
    FileHandle.standardError.write(Data((error.localizedDescription + "\n").utf8))
    exit(1)
}
