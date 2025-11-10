package de.berlin.htw.util;

public class ParsingHelper {

    public static Integer tryInt(String[] a, int i) {
        return (i < a.length && !a[i].isEmpty()) ? tryInt(a[i]) : null;
    }

    public static Integer tryInt(String s) {
        try {
            return Integer.valueOf(s);
        } catch (Exception e) {
            System.out.println("Failed to parse int: " + s);
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static Long tryL(String[] a, int i) {
        return (i < a.length && !a[i].isEmpty()) ? tryL(a[i]) : null;
    }

    public static Long tryL(String s) {
        try {
            return Long.valueOf(s);
        } catch (Exception e) {
            return null;
        }
    }

    public static Double tryD(String[] a, int i) {
        return (i < a.length && !a[i].isEmpty()) ? tryD(a[i]) : null;
    }

    public static Double tryD(String s) {
        try {
            return Double.valueOf(s);
        } catch (Exception e) {
            return null;
        }
    }
}
