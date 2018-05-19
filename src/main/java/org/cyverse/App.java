package org.cyverse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.packinstr.DataObjInp;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.pub.io.IRODSFileInputStream;
import org.irods.jargon.core.pub.io.IRODSFileOutputStream;
import org.irods.jargon.core.pub.io.PackingIrodsInputStream;
import org.irods.jargon.core.pub.io.PackingIrodsOutputStream;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class App {

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("h", "host", true, "iRODS host");
        options.addOption("p", "port", true, "iRODS port");
        options.addOption("u", "user", true, "iRODS user");
        options.addOption("z", "zone", true, "iRODS zone");
        options.addOption("t", "target", true, "target path");
        options.addOption("s", "source", true, "source path");
        return options;
    }

    public static CommandLine parseCommandLine(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = buildOptions();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return line;
    }

    private static String readPassword() {
        char[] password;

        Console console = System.console();
        if (console != null) {
            password = console.readPassword("%s: ", "Password");
            return new String(password);
        }

        return "";
    }

    private static IRODSAccount buildIrodsAccount(CommandLine commandLine) throws JargonException {
        String host = commandLine.getOptionValue('h', "localhost");
        int port = Integer.parseInt(commandLine.getOptionValue('p', "1247"));
        String user = commandLine.getOptionValue('u', "anonymous");
        String pass = readPassword();
        String zone = commandLine.getOptionValue('z', "demoZone");
        String home = "/" + zone + "/home";
        String resource = "";
        return IRODSAccount.instance(host, port, user, pass, home, zone, resource);
    }

    private static IRODSAccount authenticate(IRODSAccount account, IRODSAccessObjectFactory aof)
            throws JargonException {
        return aof.authenticateIRODSAccount(account).getAuthenticatedIRODSAccount();
    }

    // Note: a PackingIrodsOutputStream is intended to send optimally sized packets to iRODS.
    // It's not really relevant in this case, but it may be in more general cases.
    private static OutputStream getOutputStream(String path, IRODSFileFactory fileFactory)
            throws JargonException, IOException {
        DataObjInp.OpenFlags flags = DataObjInp.OpenFlags.WRITE_TRUNCATE;
        IRODSFileOutputStream outputStream = fileFactory.instanceIRODSFileOutputStream(path, flags);
        return new PackingIrodsOutputStream(outputStream);
    }

    private static void putFile(CommandLine commandLine, IRODSFileFactory fileFactory)
            throws JargonException, IOException {
        String targetPath = commandLine.getOptionValue('t', "/path/to/target");
        try (OutputStream out = getOutputStream(targetPath, fileFactory)) {
            out.write("This is a test of the iRODS Jargon System.\n".getBytes());
        }
    }

    // Note: a PackingIrodsInputStream is intended to retrieve optimally sized packets from iRODS.
    // It's not really relevant in this case, but it may be in more general cases.
    private static InputStream getInputStream(String path, IRODSFileFactory fileFactory)
            throws JargonException {
        IRODSFileInputStream inputStream = fileFactory.instanceIRODSFileInputStream(path);
        return new PackingIrodsInputStream(inputStream);
    }

    private static void getFile(CommandLine commandLine, IRODSFileFactory fileFactory)
            throws JargonException, IOException {
        String sourcePath = commandLine.getOptionValue('s',"/path/to/source");
        try (InputStream in = getInputStream(sourcePath, fileFactory)) {
            IOUtils.copy(in, System.out);
        }
    }

    public static void main(String[] args) {
        CommandLine commandLine = parseCommandLine(args);
        try {
            IRODSAccessObjectFactory aof = IRODSFileSystem.instance().getIRODSAccessObjectFactory();
            IRODSAccount account = authenticate(buildIrodsAccount(commandLine), aof);
            IRODSFileFactory fileFactory = aof.getIRODSFileFactory(account);
            putFile(commandLine, fileFactory);
            getFile(commandLine, fileFactory);
        } catch (Exception e) {
            System.err.println("Something bad happened: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
