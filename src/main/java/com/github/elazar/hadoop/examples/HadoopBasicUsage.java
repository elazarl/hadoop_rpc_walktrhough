package com.github.elazar.hadoop.examples;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

/**
 * Show how Hadoop program sets its configuration, and thus get its user.
 */
public class HadoopBasicUsage extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println("args: " + Arrays.toString(args));
        System.exit(ToolRunner.run(new HadoopBasicUsage(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        final org.apache.hadoop.conf.Configuration conf = getConf();

        final String cwd = System.getProperty("user.dir");
        System.setProperty("java.security.krb5.conf", cwd + "/krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = UserGroupInformation.
                loginUserFromKeytabAndReturnUGI("hdfs/EXAMPLE.COM", cwd + "/keytab");


        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                System.out.println(">>" + UserGroupInformation.getCurrentUser().getAuthenticationMethod());
                final FileSystem fs = FileSystem.get(conf);
                int i=0;
                final FSDataOutputStream out = fs.create(new Path("/foo_" + System.currentTimeMillis()));
                out.write("foo".getBytes(Charsets.US_ASCII));
                out.close();

                for (FileStatus status : fs.listStatus(new Path("/"))) {
                    System.out.println(status.getPath());
                    System.out.println(status);
                    if (i++ > 10) {
                        System.out.println("only first 10 showed...");
                        break;
                    }
                }
                return null;
            }
        });

        return 0;
    }
}
