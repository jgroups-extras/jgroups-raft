package org.jgroups.raft.util;

import org.jgroups.annotations.Component;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

/**
 * Iterates over all concrete Protocol classes and creates tables with Protocol's properties.
 * These tables are in turn then merged into asciidoc.
 *
 * Iterates over unsupported and experimental classes and creates tables listing those classes.
 * These tables are in turn then merged into asciidoc.
 *
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class PropsToAsciidoc {

    protected static final String ROOT_PACKAGE="org.jgroups";

    public static void main(String[] args) {
        if (args.length != 1) {
            help();
            System.err.println("args[0]=" + args[0]);
            return;
        }
        String prot_file = args[0];
        String temp_file = prot_file.replace("template", "generated");

        try {
            // first copy protocols-template.adoc file into protocols-template.adoc.xml
            File f = new File(temp_file);
            copy(new FileReader(prot_file), new FileWriter(f));

            ClassLoader cl=Thread.currentThread().getContextClassLoader();
            Set<Class<?>> classes=Util.findClassesAssignableFrom("org.jgroups.protocols.raft", Protocol.class, cl);
            // classes.addAll(Util.findClassesAssignableFrom("org.jgroups.protocols.pbcast",Protocol.class));
            Properties props = new Properties();
            for(Class<?> clazz: classes)
                convertClassToAsciidocTable(props, clazz, null);

            try(InputStream in=new FileInputStream(prot_file); OutputStream out=new FileOutputStream(temp_file)) {
                replaceVariables(in, out, props);
            }

            props=new Properties();
            List<Class<?>> unsupportedClasses = Util.findClassesAnnotatedWith("org.jgroups.protocols.raft",Unsupported.class);
            convertUnsupportedToAsciidocTable(props,unsupportedClasses,"Unsupported");
            List<Class<?>> experimentalClasses = Util.findClassesAnnotatedWith("org.jgroups.protocols.raft", Experimental.class);
            convertUnsupportedToAsciidocTable(props,experimentalClasses,"Experimental");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("PropsToAsciidoc <path to protocols-template.adoc file>");
    }


    protected static void convertUnsupportedToAsciidocTable(Properties props, List<Class<?>> clazzes, String title)
            throws ParserConfigurationException, TransformerException {
        List<String[]> rows=new ArrayList<>(clazzes.size() +1);
        rows.add(new String[]{"Package","Class"}); // add column titles first
        for(Class<?> clazz: clazzes)
            rows.add(new String[]{clazz.getPackage().getName(), clazz.getSimpleName()});

        String tmp=createAsciidocTable(rows, title, "[align=\"left\",width=\"50%\",options=\"header\"]");

        // do we have more than one property (superclass Protocol has only one property (stats))
        if (clazzes.size() > 1) {
            props.put(title, tmp);
        }
    }

    /** Creates an AsciiDoc table of the elements in rows. The first tuple needs to be the column names, the
     * rest the contents */
    protected static String createAsciidocTable(List<String[]> rows, String title, String header)
            throws ParserConfigurationException, TransformerException {
        StringBuilder sb=new StringBuilder(".").append(title).append("\n")
                .append(header).append("\n")
                .append("|=================\n");
        for(String[] row: rows) {
            for(String el: row)
                sb.append("|").append(el);
            sb.append("\n");
        }
        sb.append("|=================\n");
        return sb.toString();
    }


    private static void convertClassToAsciidocTable(final Properties props, Class<?> clazz, String prefix) throws Exception {
        if (clazz.isAnnotationPresent(Unsupported.class))
            return;

        final Map<String,String> nameToDescription=new TreeMap<>();
        getDescriptions(clazz, nameToDescription, prefix, false);

        // do we have more than one property (superclass Protocol has only one property (stats))
        if (nameToDescription.isEmpty())
            return;

        List<String[]> rows=new ArrayList<>(nameToDescription.size() +1);
        rows.add(new String[]{"Name", "Description"});
        for(Map.Entry<String,String> entry: nameToDescription.entrySet())
            rows.add(new String[]{entry.getKey(), entry.getValue()});

        String tmp=createAsciidocTable(rows, clazz.getSimpleName(), "[align=\"left\",width=\"90%\",cols=\"2,10\",options=\"header\"]");
        props.put(clazz.getSimpleName(), tmp);
    }

    protected static void getDescriptions(Class<?> clazz, final Map<String,String> m, String prefix, boolean print_class)
            throws IOException, ClassNotFoundException {
        List<Field> fields = new ArrayList<>();
        getAllFields(fields, clazz, Protocol.class);
        for (Field field : fields) {
            if (field.isAnnotationPresent(Property.class)) {
                String property = field.getName();
                Property annotation = field.getAnnotation(Property.class);
                String name=annotation.name();
                if(name != null && !name.trim().isEmpty())
                    property=name.trim();
                String desc = annotation.description();
                if(prefix != null && !prefix.isEmpty())
                    property=prefix + "." + property;
                if(print_class)
                    property=String.format("%s (%s)", property, clazz.getSimpleName());
                m.put(property, desc);
            }

            // is the field annotated with @Component?
            if(field.isAnnotationPresent(Component.class)) {
                Component ann=field.getAnnotation(Component.class);
                Class<?> type=field.getType();
                if(type.isInterface() || Modifier.isAbstract(type.getModifiers())) {
                    Set<Class<?>> implementations=Util.findClassesAssignableFrom(ROOT_PACKAGE, type, Thread.currentThread().getContextClassLoader());
                    for(Class<?> impl: implementations)
                        getDescriptions(impl, m, ann.name(), true);
                }
                else
                    getDescriptions(type, m, ann.name(), false);
            }
        }

        List<Method> methods = new ArrayList<>();
        getAllMethods(methods, clazz, Protocol.class);
        for (Method method : methods) {
            if (method.isAnnotationPresent(Property.class)) {
                Property annotation = method.getAnnotation(Property.class);
                String desc = annotation.description();

                if(desc == null || desc.isEmpty())
                    desc="n/a";

                String name = annotation.name();
                if(name.length() < 1)
                    name=Util.methodNameToAttributeName(method.getName());
                if(prefix != null && !prefix.isEmpty())
                    name=prefix + "." + name;
                if(print_class)
                    name=String.format("%s (%s)", name, clazz.getSimpleName());
                m.put(name, desc);
            }
        }
    }

    private static void getAllFields(List<Field> fields, Class<?> clazz, final Class<?> stop) {
        if (clazz.equals(stop)) return;

        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));

        if (clazz.getSuperclass() != null) {
            getAllFields(fields, clazz.getSuperclass(), stop);
        }
    }

    private static void getAllMethods(List<Method> methods, Class<?> clazz, final Class<?> stop) {
        if (clazz.equals(stop)) return;

        methods.addAll(Arrays.asList(clazz.getDeclaredMethods()));

        if (clazz.getSuperclass() != null) {
            getAllMethods(methods, clazz.getSuperclass(), stop);
        }
    }

    /**
     * Reads from the input stream and replaces occurrences of ${PROT} with p.get("PROT") and writes this to the
     * output stream. If no value is found, then the ${PROT} will simple be omitted from the output.
     * Escaped values of the form \${PROT} are not looked up and the value without the backslash will be written
     * to the output stream.
     */
    protected static void replaceVariables(InputStream in, OutputStream out, Properties p) {
        boolean looping=true;
        while(looping) {
            try {
                int ch=in.read(), n1, n2;
                if(ch == -1)
                    break;
                switch(ch) {
                    case '\\':
                        n1=in.read();
                        n2=in.read();
                        if(n1 == -1 || n2 == -1) {
                            looping=false;
                            if(n1 != -1)
                                out.write(n1);
                            break;
                        }

                        if(n1 == '$' && n2 == '{') {
                            String s=readUntilBracket(in);
                            out.write(n1);
                            out.write(n2);
                            out.write(s.getBytes());
                            out.write('}');
                        }
                        else {
                            out.write(ch);
                            out.write(n1);
                            out.write(n2);
                        }
                        break;
                    case '$':
                        n1=in.read();
                        if(n1 == -1) {
                            out.write(ch);
                            looping=false;
                        }
                        else {
                            if(n1 == '{') {
                                String s=readUntilBracket(in);
                                writeVarToStream(s, p, out);
                            }
                            else {
                                out.write(ch);
                                out.write(n1);
                            }
                        }
                        break;
                    default:
                        out.write(ch);
                }
            }
            catch(IOException e) {
                break;
            }
        }
        Util.close(in, out);
    }


    protected static void writeVarToStream(String var, Properties p, OutputStream out) throws IOException {
        String val=(String)p.get(var);
        if(val != null)
            out.write(val.getBytes());
    }

    /** Reads until the next bracket '}' and returns the string excluding the bracket, or throws an exception if
     * no bracket has been found */
    protected static String readUntilBracket(InputStream in) throws IOException {
        StringBuilder sb=new StringBuilder();
        while(true) {
            int ch=in.read();
            switch(ch) {
                case -1: throw new EOFException("no matching } found");
                case '}':
                    return sb.toString();
                default:
                    sb.append((char)ch);
            }
        }
    }

    private static String fileToString(File f) throws Exception {
        StringWriter output = new StringWriter();
        FileReader input = new FileReader(f);
        char[] buffer = new char[8 * 1024];
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toString();
    }

    public static int copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[8 * 1024];
        int count = 0;
        int n = 0;
        try {
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                count += n;
            }
        } finally {
            output.flush();
            output.close();
        }
        return count;
    }
}
