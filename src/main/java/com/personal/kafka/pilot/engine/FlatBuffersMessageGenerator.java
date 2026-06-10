package com.personal.kafka.pilot.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.personal.kafka.pilot.model.FieldConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

/**
 * Generates FlatBuffers messages for Kafka load testing.
 *
 * <p>Supports two modes:
 * <ol>
 *   <li><b>Binary mode</b> — template is a Base64-encoded FlatBuffer binary.
 *       Auto-increment / rotation mutations are applied via the generated class's
 *       {@code mutate*()} methods (requires flatbufClassName).</li>
 *   <li><b>Reflection-JSON mode</b> — template is a JSON object whose keys map
 *       to the FlatBuffer field order of the generated {@code createXxx()} static
 *       factory method.  Auto-increment / rotation are applied to the JSON before
 *       the FlatBuffer is built.</li>
 * </ol>
 *
 * <p>The produced message value is a {@code byte[]} (raw FlatBuffer bytes) which
 * the user-supplied Kafka serializer is expected to forward as-is.
 */
public class FlatBuffersMessageGenerator<T> extends MessageGenerator<T> {

    private static final Logger logger = LoggerFactory.getLogger(FlatBuffersMessageGenerator.class);

    private final ObjectMapper objectMapper;
    private final List<FieldConfig> autoIncrementFields;
    private final List<FieldConfig> rotationFields;
    private final Class<?> flatbufClass;

    /** True when the template is Base64-encoded binary; false for JSON mode. */
    private final boolean binaryMode;

    /** Decoded base template bytes (binary mode). */
    private final byte[] binaryTemplate;

    /** Parsed JSON template (reflection-JSON mode). */
    private final JsonNode jsonTemplate;

    // --- Library-backed serialization (mirrors CloudView AlertingEventService) ---
    /**
     * When non-null, messages are serialized exactly like CloudView's
     * {@code AlertingEventService.produceAlertingEvent}: a POJO entity is bound
     * from the JSON template and converted to FlatBuffer bytes via
     * {@code com.qualys.flatbuff.utils.FlatBufferUtils#getFlatBuffer(Object)}.
     */
    private final Object flatBufferUtils;
    private final Method getFlatBufferMethod;
    private final Class<?> pojoEntityClass;
    private final boolean useLibrarySerialization;

    public FlatBuffersMessageGenerator(String template,
                                       List<FieldConfig> autoIncrementFields,
                                       List<FieldConfig> rotationFields,
                                       Class<?> flatbufClass) {
        this(template, autoIncrementFields, rotationFields, flatbufClass, null, null);
    }

    /**
     * @param template       JSON template (or Base64 FlatBuffer binary)
     * @param flatbufClass   In library mode this is the POJO <b>entity</b> class
     *                       (e.g. {@code ...flatbuff.entity.AWSMonitorControlEvaluationDocument}).
     *                       In legacy/binary mode it is the generated FlatBuffers Table class.
     * @param loader         Classloader holding {@code FlatBufferUtils}, the entity and the
     *                       generated {@code dto} classes (typically the Maven/custom classloader).
     * @param daoPackage     The package containing the generated FlatBuffers {@code dto} classes
     *                       (e.g. {@code com.qualys.cloudview.flatbuff.dto}). Required for library mode.
     */
    public FlatBuffersMessageGenerator(String template,
                                       List<FieldConfig> autoIncrementFields,
                                       List<FieldConfig> rotationFields,
                                       Class<?> flatbufClass,
                                       ClassLoader loader,
                                       String daoPackage) {
        super(template, autoIncrementFields, rotationFields);
        this.objectMapper = new ObjectMapper();
        this.autoIncrementFields = autoIncrementFields;
        this.rotationFields = rotationFields;
        this.flatbufClass = flatbufClass;
        this.binaryMode = looksLikeBase64(template.trim());

        if (binaryMode) {
            this.binaryTemplate = Base64.getDecoder().decode(template.trim());
            this.jsonTemplate = null;
            logger.info("FlatBuffersMessageGenerator: binary mode, {} bytes", binaryTemplate.length);
        } else {
            try {
                this.jsonTemplate = objectMapper.readTree(template);
            } catch (Exception e) {
                throw new RuntimeException("FlatBuffers template is neither valid Base64 nor valid JSON", e);
            }
            this.binaryTemplate = null;
            logger.info("FlatBuffersMessageGenerator: reflection-JSON mode");
        }

        // Attempt to wire the CloudView FlatBufferUtils library (preferred path).
        Object utils = null;
        Method getFb = null;
        Class<?> pojo = null;
        if (!binaryMode && flatbufClass != null && loader != null && daoPackage != null && !daoPackage.trim().isEmpty()) {
            try {
                Class<?> configClass = Class.forName("com.qualys.flatbuff.config.FlatBuffConfig", true, loader);
                Class<?> utilsClass = Class.forName("com.qualys.flatbuff.utils.FlatBufferUtils", true, loader);
                Object config = configClass.getDeclaredConstructor().newInstance();
                configClass.getMethod("setDaoPackage", String.class).invoke(config, daoPackage.trim());
                utils = utilsClass.getDeclaredConstructor(configClass).newInstance(config);
                getFb = utilsClass.getMethod("getFlatBuffer", Object.class);
                pojo = flatbufClass;
                logger.info("FlatBuffersMessageGenerator: using FlatBufferUtils library — entity={}, daoPackage={}",
                        pojo.getName(), daoPackage.trim());
            } catch (Throwable t) {
                logger.warn("FlatBufferUtils library not available ({}); falling back to reflection-JSON build",
                        t.toString());
                utils = null;
                getFb = null;
                pojo = null;
            }
        }
        this.flatBufferUtils = utils;
        this.getFlatBufferMethod = getFb;
        this.pojoEntityClass = pojo;
        this.useLibrarySerialization = utils != null && getFb != null && pojo != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] generateMessageAndJson(int messageIndex) {
        try {
            Object[] result;
            if (binaryMode) {
                result = generateFromBinary(messageIndex);
            } else {
                result = generateFromJson(messageIndex);
            }
            // Debug: Log what we're actually producing
            Object message = result[0];
            logger.debug("Generated message #{}: type={}, length={}", 
                messageIndex, message.getClass().getSimpleName(), 
                message instanceof byte[] ? ((byte[]) message).length : message.toString().length());
            return result;
        } catch (Exception e) {
            logger.error("Error generating FlatBuffers message #{}: {}", messageIndex, e.getMessage(), e);
            throw new RuntimeException("Failed to generate FlatBuffers message", e);
        }
    }

    // -------------------------------------------------------------------------
    // Binary mode
    // -------------------------------------------------------------------------

    private Object[] generateFromBinary(int messageIndex) throws Exception {
        byte[] bytes = binaryTemplate.clone();

        if (flatbufClass != null) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            Object root = invokeGetRootAs(buf);
            if (root != null) {
                applyMutations(root, autoIncrementFields);
                applyMutations(root, rotationFields);
                // Re-read mutated bytes
                bytes = buf.array();
            }
        }

        String displayJson = Base64.getEncoder().encodeToString(bytes);
        return new Object[]{bytes, displayJson};
    }

    private Object invokeGetRootAs(ByteBuffer buf) {
        if (flatbufClass == null) return null;
        try {
            String simpleName = flatbufClass.getSimpleName();
            Method m = flatbufClass.getMethod("getRootAs" + simpleName, ByteBuffer.class);
            return m.invoke(null, buf);
        } catch (Exception e) {
            logger.warn("Could not invoke getRootAs on {}: {}", flatbufClass.getSimpleName(), e.getMessage());
            return null;
        }
    }

    private void applyMutations(Object root, List<FieldConfig> fields) {
        if (fields == null || root == null) return;
        for (FieldConfig fc : fields) {
            Object value = fc.getNextValue();
            String fieldName = fc.getFieldPath();
            if (fieldName == null || fieldName.contains(".")) continue; // FlatBuffers mutators are top-level only
            try {
                String mutatorName = "mutate" + capitalize(fieldName);
                for (Method m : root.getClass().getMethods()) {
                    if (m.getName().equals(mutatorName) && m.getParameterCount() == 1) {
                        m.invoke(root, coerce(value, m.getParameterTypes()[0]));
                        break;
                    }
                }
            } catch (Exception e) {
                logger.warn("Could not mutate field '{}': {}", fieldName, e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Reflection-JSON mode
    // -------------------------------------------------------------------------

    private Object[] generateFromJson(int messageIndex) throws Exception {
        ObjectNode node = objectMapper.createObjectNode();
        node.setAll((ObjectNode) jsonTemplate);

        applyFieldConfigs(node, autoIncrementFields);
        applyFieldConfigs(node, rotationFields);

        String json = objectMapper.writeValueAsString(node);

        // Preferred path: replicate CloudView AlertingEventService — bind the JSON
        // into the POJO entity and serialize via FlatBufferUtils.getFlatBuffer(pojo).
        if (useLibrarySerialization) {
            Object pojo = objectMapper.readValue(json, pojoEntityClass);
            byte[] flatBytes = (byte[]) getFlatBufferMethod.invoke(flatBufferUtils, pojo);
            return new Object[]{flatBytes, json};
        }

        if (flatbufClass != null) {
            byte[] flatBytes = buildFlatBufferFromJson(node, flatbufClass);
            if (flatBytes != null) return new Object[]{flatBytes, json};
            // Do NOT silently fall back to JSON bytes — that produces a wrong-format message.
            throw new IllegalStateException("FlatBuffers encoding failed for class " + flatbufClass.getName()
                    + ". Could not load the FlatBufferUtils library from the configured Maven dependency, and the "
                    + "fallback binary builder produced no output. Ensure the Maven dependency includes "
                    + "flatbuffer-service (com.qualys.flatbuff.utils.FlatBufferUtils) and the generated dto classes.");
        }

        // FlatBuffer mode with nothing configured to encode with — fail loudly rather than
        // pushing plain JSON bytes that downstream FlatBuffer consumers cannot read.
        throw new IllegalStateException("FlatBuffers mode is enabled but no FlatBuffers class is configured. "
                + "Set the FlatBuf class name to the entity (e.g. "
                + "com.qualys.cloudview.flatbuff.entity.AWSMonitorControlEvaluationDocument) and configure a Maven "
                + "dependency that provides flatbuffer-service plus the generated dto classes.");
    }

    /**
     * Builds a real FlatBuffer binary from a JSON ObjectNode using the generated
     * static {@code createXxx(FlatBufferBuilder, ...)} method via reflection.
     * Strings are pre-created with builder.createString(); scalars and booleans
     * are passed directly. Vector fields (arrays) are passed as empty (offset 0)
     * when not present in the JSON.
     */
    private byte[] buildFlatBufferFromJson(ObjectNode node, Class<?> clazz) throws Exception {
        // Find the create method: static, first param is FlatBufferBuilder
        java.lang.reflect.Method createMethod = null;
        for (java.lang.reflect.Method m : clazz.getMethods()) {
            if (!java.lang.reflect.Modifier.isStatic(m.getModifiers())) continue;
            if (!m.getName().startsWith("create")) continue;
            Class<?>[] params = m.getParameterTypes();
            if (params.length < 2) continue;
            if (params[0].getName().equals("com.google.flatbuffers.FlatBufferBuilder")) {
                createMethod = m;
                break;
            }
        }
        if (createMethod == null) {
            logger.warn("No create method found on {}", clazz.getSimpleName());
            return null;
        }

        // Map parameter names from addXxx static methods: addUuid(builder, offset) → field "uuid"
        // The create method param order matches the addXxx field index order (schema order)
        Class<?>[] paramTypes = createMethod.getParameterTypes();
        // Derive field names from addXxx methods in declaration order using slot scan (same as KafkaSearchService)
        java.util.List<String> fieldNames = deriveFieldNamesFromClass(clazz, paramTypes.length - 1);

        // Load FlatBufferBuilder class from same classloader as the generated class
        ClassLoader loader = clazz.getClassLoader();
        Class<?> builderClass = loader.loadClass("com.google.flatbuffers.FlatBufferBuilder");
        Object builder = builderClass.getConstructor().newInstance();
        java.lang.reflect.Method createString = builderClass.getMethod("createString", String.class);
        java.lang.reflect.Method finishMethod = null;
        for (java.lang.reflect.Method m : clazz.getMethods()) {
            if (m.getName().startsWith("finish") && m.getName().endsWith("Buffer")
                    && java.lang.reflect.Modifier.isStatic(m.getModifiers())
                    && m.getParameterCount() == 2) {
                finishMethod = m;
                break;
            }
        }
        java.lang.reflect.Method sizedByteArray = builderClass.getMethod("sizedByteArray");

        // Pre-create all string offsets (must be done before startObject)
        Object[] args = new Object[paramTypes.length];
        args[0] = builder;
        for (int i = 0; i < fieldNames.size() && i + 1 < paramTypes.length; i++) {
            String fieldName = fieldNames.get(i);
            Class<?> pType = paramTypes[i + 1];
            JsonNode val = node.get(fieldName);
            if (pType == int.class && val != null && val.isTextual()) {
                // int param for string fields = pre-created offset
                args[i + 1] = (int) createString.invoke(builder, val.asText());
            } else if (pType == int.class) {
                args[i + 1] = 0; // empty vector or missing string
            } else if (pType == long.class) {
                args[i + 1] = val != null ? val.asLong() : 0L;
            } else if (pType == boolean.class) {
                args[i + 1] = val != null && val.asBoolean();
            } else if (pType == int.class) {
                args[i + 1] = val != null ? val.asInt() : 0;
            } else if (pType == float.class) {
                args[i + 1] = val != null ? (float) val.asDouble() : 0f;
            } else if (pType == double.class) {
                args[i + 1] = val != null ? val.asDouble() : 0.0;
            } else if (pType == short.class) {
                args[i + 1] = val != null ? (short) val.asInt() : (short) 0;
            } else if (pType == byte.class) {
                args[i + 1] = val != null ? (byte) val.asInt() : (byte) 0;
            } else {
                args[i + 1] = 0;
            }
        }

        int rootOffset = (int) createMethod.invoke(null, args);

        if (finishMethod != null) {
            finishMethod.invoke(null, builder, rootOffset);
        } else {
            java.lang.reflect.Method finish = builderClass.getMethod("finish", int.class);
            finish.invoke(builder, rootOffset);
        }

        return (byte[]) sizedByteArray.invoke(builder);
    }

    /**
     * Derives field names in schema order directly from the create method's parameter names.
     * Java 8+ compiled with -parameters flag exposes parameter names; if not available,
     * we fall back to matching addXxx methods by their second parameter type against the
     * create method's parameter types in order.
     */
    private java.util.List<String> deriveFieldNamesFromClass(Class<?> clazz, int expectedCount) {
        // Build map: type-signature → list of field names from addXxx(FlatBufferBuilder, T)
        // We need ordered names matching createXxx(FlatBufferBuilder, T1, T2, ...) parameters
        // Strategy: use the create method's parameter names if available (-parameters compile flag)
        java.lang.reflect.Method createMethod = null;
        for (java.lang.reflect.Method m : clazz.getMethods()) {
            if (!java.lang.reflect.Modifier.isStatic(m.getModifiers())) continue;
            if (!m.getName().startsWith("create")) continue;
            Class<?>[] p = m.getParameterTypes();
            if (p.length >= 2 && p[0].getName().equals("com.google.flatbuffers.FlatBufferBuilder")) {
                createMethod = m;
                break;
            }
        }
        if (createMethod == null) return java.util.Collections.emptyList();

        java.lang.reflect.Parameter[] params = createMethod.getParameters();
        java.util.List<String> names = new java.util.ArrayList<>();
        // Skip first param (FlatBufferBuilder)
        for (int i = 1; i < params.length; i++) {
            String pName = params[i].getName();
            // If compiled with -parameters: name is e.g. "uuidOffset", "cid", "remediationEnabled"
            // Strip "Offset" suffix to get the field name
            if (pName.endsWith("Offset")) pName = pName.substring(0, pName.length() - 6);
            names.add(pName);
        }
        return names;
    }

    private void applyFieldConfigs(ObjectNode node, List<FieldConfig> fields) {
        if (fields == null) return;
        for (FieldConfig fc : fields) {
            Object value = fc.getNextValue();
            setFieldValue(node, fc.getFieldPath(), value);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static boolean looksLikeBase64(String s) {
        if (s == null || s.isEmpty()) return false;
        return s.matches("^[A-Za-z0-9+/=\\r\\n]+$") && !s.trim().startsWith("{");
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static Object coerce(Object value, Class<?> targetType) {
        if (value == null) return defaultForType(targetType);
        if (targetType == String.class) return value.toString();
        if (targetType == boolean.class || targetType == Boolean.class) return Boolean.parseBoolean(value.toString());
        if (value instanceof Number) {
            Number n = (Number) value;
            if (targetType == int.class || targetType == Integer.class) return n.intValue();
            if (targetType == long.class || targetType == Long.class) return n.longValue();
            if (targetType == short.class || targetType == Short.class) return n.shortValue();
            if (targetType == byte.class || targetType == Byte.class) return n.byteValue();
            if (targetType == float.class || targetType == Float.class) return n.floatValue();
            if (targetType == double.class || targetType == Double.class) return n.doubleValue();
        }
        if (value instanceof String) {
            String s = (String) value;
            try {
                if (targetType == int.class || targetType == Integer.class) return Integer.parseInt(s);
                if (targetType == long.class || targetType == Long.class) return Long.parseLong(s);
                if (targetType == short.class || targetType == Short.class) return Short.parseShort(s);
                if (targetType == byte.class || targetType == Byte.class) return Byte.parseByte(s);
                if (targetType == float.class || targetType == Float.class) return Float.parseFloat(s);
                if (targetType == double.class || targetType == Double.class) return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                return defaultForType(targetType);
            }
        }
        if (targetType.isInstance(value)) return value;
        return defaultForType(targetType);
    }

    private static Object defaultForType(Class<?> t) {
        if (t == int.class || t == Integer.class) return 0;
        if (t == long.class || t == Long.class) return 0L;
        if (t == short.class || t == Short.class) return (short) 0;
        if (t == byte.class || t == Byte.class) return (byte) 0;
        if (t == float.class || t == Float.class) return 0f;
        if (t == double.class || t == Double.class) return 0.0;
        if (t == boolean.class || t == Boolean.class) return false;
        return null;
    }
}
