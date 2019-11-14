
require(tesseract)
require(pdftools)
# script.dir <- dirname(sys.frame(0)$ofile) 
text <- ocr("example_ocr.png")
cat(text) 

pngfile <- pdftools::pdf_convert('example_ocr.pdf', dpi = 600)
text2 <- ocr(pngfile)
cat(text2)

# filename = paste("DUMP_ocr.txt", sep = "")
# write(txt, filename)

# check out https://github.com/cloudyr/RoogleVision