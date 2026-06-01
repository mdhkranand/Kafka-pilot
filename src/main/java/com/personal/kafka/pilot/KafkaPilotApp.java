package com.personal.kafka.pilot;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPilotApp extends Application {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPilotApp.class);

    @Override
    public void start(Stage primaryStage) {
        try {
            logger.info("Starting Kafka Pilot Application");

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main.fxml"));
            Parent root = loader.load();

            Scene scene = new Scene(root);
            scene.getStylesheets().add(getClass().getResource("/styles/app.css").toExternalForm());

            primaryStage.setTitle("Kafka Pilot");
            primaryStage.setScene(scene);
            primaryStage.setMinWidth(1000);
            primaryStage.setMinHeight(700);

            // Set application icon
            try {
                Image icon = new Image(getClass().getResourceAsStream("/logo.png"));
                primaryStage.getIcons().add(icon);
            } catch (Exception e) {
                logger.warn("Could not load application icon", e);
            }

            primaryStage.setOnCloseRequest(event -> {
                logger.info("Application close requested");
                deleteCgCacheFiles(); // cleanup before exit
                javafx.application.Platform.exit();
                System.exit(0);
            });
            primaryStage.show();

            logger.info("Application started successfully");

        } catch (Exception e) {
            logger.error("Failed to start application", e);
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        logger.info("Application stopping");
        deleteCgCacheFiles();
    }

    private void deleteCgCacheFiles() {
        try {
            java.nio.file.Path configDir = java.nio.file.Paths.get(
                System.getProperty("user.dir"), "kafka-pilot-configs");
            if (!java.nio.file.Files.exists(configDir)) return;
            java.nio.file.Files.list(configDir)
                .filter(p -> p.getFileName().toString().startsWith("cg-cache-")
                          && p.getFileName().toString().endsWith(".json"))
                .forEach(p -> {
                    try {
                        java.nio.file.Files.delete(p);
                        logger.info("Deleted cache file: {}", p.getFileName());
                    } catch (Exception e) {
                        logger.warn("Could not delete cache file {}: {}", p.getFileName(), e.getMessage());
                    }
                });
        } catch (Exception e) {
            logger.warn("Error cleaning up cache files: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        logger.info("Launching Kafka Pilot");
        launch(args);
    }
}
