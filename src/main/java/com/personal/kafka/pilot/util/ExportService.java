package com.personal.kafka.pilot.util;

import javafx.scene.control.TableView;
import javafx.scene.chart.LineChart;
import javafx.scene.SnapshotParameters;
import javafx.scene.image.WritableImage;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import javafx.scene.image.PixelReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Service for exporting table data and charts to various formats
 */
public class ExportService {
    
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    
    /**
     * Export TableView data to CSV format
     */
    public void exportTableViewToCSV(TableView<?> table, String filename) throws IOException {
        StringBuilder csv = new StringBuilder();
        
        try {
            // Add headers
            for (int i = 0; i < table.getColumns().size(); i++) {
                if (i > 0) csv.append(",");
                csv.append(escapeCSV(table.getColumns().get(i).getText()));
            }
            csv.append("\n");
            
            // Add data rows
            for (Object item : table.getItems()) {
                for (int i = 0; i < table.getColumns().size(); i++) {
                    if (i > 0) csv.append(",");
                    @SuppressWarnings("unchecked")
                    javafx.scene.control.TableColumn<Object, Object> column = 
                        (javafx.scene.control.TableColumn<Object, Object>) table.getColumns().get(i);
                    Object cellData = column.getCellData(item);
                    csv.append(escapeCSV(cellData != null ? cellData.toString() : ""));
                }
                csv.append("\n");
            }
            
            // Write to file and clean up
            byte[] csvBytes = csv.toString().getBytes(StandardCharsets.UTF_8);
            Files.write(Paths.get(filename), csvBytes);
            
        } finally {
            // Clear the StringBuilder to free memory
            csv.setLength(0);
        }
    }
    
    /**
     * Export LineChart to PNG image
     */
    public void exportChartToPNG(LineChart<?, ?> chart, String filename) throws IOException {
        SnapshotParameters params = new SnapshotParameters();
        WritableImage image = null;
        BufferedImage bufferedImage = null;
        PixelReader pixelReader = null;
        
        try {
            image = chart.snapshot(params, null);
            
            // Convert JavaFX image to BufferedImage
            int width = (int) image.getWidth();
            int height = (int) image.getHeight();
            bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            
            pixelReader = image.getPixelReader();
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    javafx.scene.paint.Color color = pixelReader.getColor(x, y);
                    int argb = ((int) (color.getOpacity() * 255) << 24) |
                              ((int) (color.getRed() * 255) << 16) |
                              ((int) (color.getGreen() * 255) << 8) |
                              ((int) (color.getBlue() * 255));
                    bufferedImage.setRGB(x, y, argb);
                }
            }
            
            File file = new File(filename);
            ImageIO.write(bufferedImage, "PNG", file);
            
        } finally {
            // Clean up resources
            if (pixelReader != null) {
                pixelReader = null;
            }
            if (bufferedImage != null) {
                bufferedImage.flush();
                bufferedImage = null;
            }
            image = null;
            // Suggest garbage collection for large images
            System.gc();
        }
    }
    
    /**
     * Create a FileChooser for CSV export
     */
    public FileChooser createCSVFileChooser(String defaultName) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Export to CSV");
        fileChooser.getExtensionFilters().add(
            new FileChooser.ExtensionFilter("CSV Files", "*.csv")
        );
        
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        fileChooser.setInitialFileName(defaultName + "_" + timestamp + ".csv");
        
        return fileChooser;
    }
    
    /**
     * Create a FileChooser for PNG export
     */
    public FileChooser createPNGFileChooser(String defaultName) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Export to PNG");
        fileChooser.getExtensionFilters().add(
            new FileChooser.ExtensionFilter("PNG Files", "*.png")
        );
        
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        fileChooser.setInitialFileName(defaultName + "_" + timestamp + ".png");
        
        return fileChooser;
    }
    
    /**
     * Create a FileChooser for text export
     */
    public FileChooser createTextFileChooser(String defaultName) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Export to Text");
        fileChooser.getExtensionFilters().add(
            new FileChooser.ExtensionFilter("Text Files", "*.txt")
        );
        
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        fileChooser.setInitialFileName(defaultName + "_" + timestamp + ".txt");
        
        return fileChooser;
    }
    
    /**
     * Escape CSV values to handle commas, quotes, and newlines
     */
    private String escapeCSV(String value) {
        if (value == null) return "";
        
        // If value contains comma, quote, or newline, wrap in quotes and escape internal quotes
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        
        return value;
    }
    
    /**
     * Generate timestamped filename
     */
    public String generateTimestampedFilename(String prefix, String extension) {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        return prefix + "_" + timestamp + "." + extension;
    }
}
