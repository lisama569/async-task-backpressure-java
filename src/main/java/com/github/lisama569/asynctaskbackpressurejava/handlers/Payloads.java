package com.github.lisama569.asynctaskbackpressurejava.handlers;

/**
 * 极简 key=value,key2=value2 payload 解析，仅用于本地演示与可重复测试。
 */
final class Payloads {

    private Payloads() {
    }

    static long parseLong(String payload, String key, long defaultValue) {
        String v = parseString(payload, key, null);
        if (v == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    static String parseString(String payload, String key, String defaultValue) {
        if (payload == null || payload.isBlank()) {
            return defaultValue;
        }
        for (String pair : payload.split(",")) {
            int idx = pair.indexOf('=');
            if (idx > 0 && pair.substring(0, idx).trim().equals(key)) {
                return pair.substring(idx + 1).trim();
            }
        }
        return defaultValue;
    }
}
