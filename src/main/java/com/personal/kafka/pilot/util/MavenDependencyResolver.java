package com.personal.kafka.pilot.util;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MavenDependencyResolver {
    
    private static final Logger logger = LoggerFactory.getLogger(MavenDependencyResolver.class);
    
    private final RepositorySystem repositorySystem;
    private final DefaultRepositorySystemSession session;
    private final List<RemoteRepository> repositories;
    
    public MavenDependencyResolver() {
        this.repositorySystem = newRepositorySystem();
        this.session = newSession(repositorySystem);
        this.repositories = createRepositories();
        
        logger.info("MavenDependencyResolver initialized");
    }
    
    public List<File> resolveDependency(String groupId, String artifactId, String version) 
            throws DependencyResolutionException {
        
        String coordinates = groupId + ":" + artifactId + ":" + version;
        logger.info("Resolving Maven dependency: {}", coordinates);
        
        Artifact artifact = new DefaultArtifact(coordinates);
        Dependency dependency = new Dependency(artifact, "compile");
        
        CollectRequest collectRequest = new CollectRequest();
        collectRequest.setRoot(dependency);
        collectRequest.setRepositories(repositories);
        
        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, null);
        
        DependencyResult result = repositorySystem.resolveDependencies(session, dependencyRequest);
        
        List<File> resolvedFiles = result.getArtifactResults().stream()
            .map(ArtifactResult::getArtifact)
            .map(Artifact::getFile)
            .collect(Collectors.toList());
        
        logger.info("Resolved {} artifacts for {}", resolvedFiles.size(), coordinates);
        return resolvedFiles;
    }
    
    public URLClassLoader loadDependencyIntoClassLoader(String groupId, String artifactId, String version) 
            throws Exception {
        
        List<File> files = resolveDependency(groupId, artifactId, version);
        
        URL[] urls = new URL[files.size()];
        for (int i = 0; i < files.size(); i++) {
            urls[i] = files.get(i).toURI().toURL();
            logger.info("Adding to classpath: {}", files.get(i).getAbsolutePath());
        }
        
        URLClassLoader classLoader = new URLClassLoader(
            urls,
            Thread.currentThread().getContextClassLoader()
        );
        
        logger.info("Created URLClassLoader with {} JARs", urls.length);
        return classLoader;
    }

    /**
     * Loads multiple dependency JARs into a single URLClassLoader.
     * @param files List of JAR files to load
     * @return URLClassLoader containing all JARs
     * @throws Exception if loading fails
     */
    public URLClassLoader loadDependenciesIntoClassLoader(List<File> files)
            throws Exception {

        URL[] urls = new URL[files.size()];
        for (int i = 0; i < files.size(); i++) {
            urls[i] = files.get(i).toURI().toURL();
            logger.info("Adding to classpath: {}", files.get(i).getAbsolutePath());
        }

        URLClassLoader classLoader = new URLClassLoader(
            urls,
            Thread.currentThread().getContextClassLoader()
        );

        logger.info("Created URLClassLoader with {} JARs", urls.length);
        return classLoader;
    }

    private RepositorySystem newRepositorySystem() {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        
        return locator.getService(RepositorySystem.class);
    }
    
    private DefaultRepositorySystemSession newSession(RepositorySystem system) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        
        String userHome = System.getProperty("user.home");
        LocalRepository localRepo = new LocalRepository(userHome + "/.m2/repository");
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
        
        logger.info("Using local Maven repository: {}", localRepo.getBasedir());
        
        return session;
    }
    
    private List<RemoteRepository> createRepositories() {
        List<RemoteRepository> repos = new ArrayList<>();
        
        repos.add(new RemoteRepository.Builder("central", "default", 
            "https://repo.maven.apache.org/maven2/").build());
        
        repos.add(new RemoteRepository.Builder("confluent", "default",
            "https://packages.confluent.io/maven/").build());
        
        logger.info("Configured {} remote repositories", repos.size());
        return repos;
    }
    
    public static class MavenDependency {
        private String groupId;
        private String artifactId;
        private String version;
        
        public MavenDependency(String groupId, String artifactId, String version) {
            this.groupId = groupId;
            this.artifactId = artifactId;
            this.version = version;
        }
        
        public String getGroupId() { return groupId; }
        public String getArtifactId() { return artifactId; }
        public String getVersion() { return version; }
        
        @Override
        public String toString() {
            return groupId + ":" + artifactId + ":" + version;
        }
        
        public static MavenDependency parse(String coordinates) {
            String[] parts = coordinates.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException(
                    "Invalid Maven coordinates. Expected format: groupId:artifactId:version");
            }
            return new MavenDependency(parts[0].trim(), parts[1].trim(), parts[2].trim());
        }
    }
}
