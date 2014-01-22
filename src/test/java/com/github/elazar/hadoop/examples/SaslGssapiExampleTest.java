package com.github.elazar.hadoop.examples;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.Ignore;
import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.*;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * Show simple use of GSSAPI SASL authentication
 */
@Ignore
public class SaslGssapiExampleTest {

    @Test
    public void testSaslGssapi() throws Exception {
        final MiniKdc kdc = makeKdc();
        kdc.start();

        try {
            Subject serverSubject = getServerKdcSubject(kdc);
            Subject clientSubject = SubjectgetClientKdcSubject(kdc);

            final SaslClient client = Subject.doAs(clientSubject, new PrivilegedAction<SaslClient>() {
                @Override
                public SaslClient run() {
                    try {
                        return Sasl.createSaslClient(
                                new String[]{"GSSAPI"},
                                // authzId is not needed in general,
                                // see http://www.openldap.org/lists/openldap-devel/200011/msg00036.html
                                null,
                                "server", kdc.getRealm(),
                                null, null
                        );
                    } catch (SaslException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            SaslServer server = Subject.doAs(serverSubject, new PrivilegedAction<SaslServer>() {
                @Override
                public SaslServer run() {
                    try {
                        return Sasl.createSaslServer("GSSAPI", "server", kdc.getRealm(), null, new CallbackHandler() {
                            @Override
                            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                                for (Callback callback : callbacks) {
                                    if (callback instanceof AuthorizeCallback) {
                                        final AuthorizeCallback authorizeCb = (AuthorizeCallback) callback;
                                        final String user =
                                                authorizeCb.getAuthenticationID();
                                        if (user.equals("client@" + kdc.getRealm())) {
                                            authorizeCb.setAuthorized(true);
                                        } else {
                                            authorizeCb.setAuthorized(false);
                                        }
                                    }
                                }
                            }
                        });
                    } catch (SaslException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            // We know that in GSSAPI client has initial response
            assertThat(client.hasInitialResponse(), is(true));
            byte[] resp = new byte[0];
            int nclient = 0, nserver = 0;
            while (!client.isComplete() && !server.isComplete()) {
                if (!client.isComplete()) {
                    nclient++;
                    resp = evaluateChallengeAs(clientSubject, client, resp);
                }
                if (!server.isComplete()) {
                    nserver++;
                    resp = evaluateResponseAs(serverSubject, server, resp);
                }
                // cast to long since Math.abs(Integer.MIN_VALUE) < 0.
                assertThat(Math.abs((long)(nclient-nserver)), lessThanOrEqualTo(1L));
            }
            assertThat(server.isComplete(), is(true));
            assertThat(client.isComplete(), is(true));
        } finally {
            kdc.stop();
        }
    }

    private byte[] evaluateResponseAs(final Subject serverSubject, final SaslServer server, final byte[] challenge) {
        return (byte[]) Subject.doAs(serverSubject, new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    return server.evaluateResponse(challenge);
                } catch (SaslException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private byte[] evaluateChallengeAs(Subject clientSubject, final SaslClient client, final byte[] challenge) {
        return (byte[]) Subject.doAs(clientSubject, new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    return client.evaluateChallenge(challenge);
                } catch (SaslException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private Subject SubjectgetClientKdcSubject(final MiniKdc kdc) throws Exception {
        final String principal = "client";
        final String password = "secret pass";
        kdc.createPrincipal(principal, password);

        Configuration conf = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(getKrb5LoginModuleName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                ImmutableMap.<String, String>builder().
                                        put("debug", "true").
                                        put("principal", principal + "@" + kdc.getRealm()).
                                        put("useKeyTab", "false").
                                        build())
                };
            }
        };
        LoginContext login = new LoginContext("", new Subject(),
                new CallbackHandler() {
                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        System.out.println(Arrays.toString(callbacks));
                        for (Callback callback : callbacks) {
                            if (callback instanceof PasswordCallback) {
                                ((PasswordCallback) callback).setPassword(password.toCharArray());
                            } else if (callback instanceof NameCallback) {
                                ((NameCallback) callback).setName("client@" + kdc.getRealm());
                            }
                        }
                    }
                },
                conf);
        login.login();
        return login.getSubject();
    }

    private Subject getServerKdcSubject(final MiniKdc kdc) throws Exception {
        // for some reason, Java appends "/realm" to the principal name given to it
        // in the createSasl* methods. You can probably override this behaviour by
        // setting an option to the SaslClient or to the JAAS login module, but I'm
        // too exhausted figuring out how to do that.
        final String principal = "server/" + kdc.getRealm().toLowerCase();
        final File keyTab = new File(getWorkingDir(), "keyTab");
        kdc.createPrincipal(keyTab, principal);

        Configuration conf = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(getKrb5LoginModuleName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                ImmutableMap.<String, String>builder().
                                        put("debug", "true").
                                        put("principal", principal + "@" + kdc.getRealm()).
                                        put("useKeyTab", "true").
                                        put("keyTab", keyTab.getAbsolutePath()).
                                        put("storeKey", "true").
                                        build())
                };
            }
        };
        LoginContext login = new LoginContext("", new Subject(), null, conf);
        login.login();
        return login.getSubject();
    }

    public static String getKrb5LoginModuleName() {
        return System.getProperty("java.vendor").contains("IBM")
                ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
    }

    private MiniKdc makeKdc() throws Exception {
        return new MiniKdc(MiniKdc.createConf(), getWorkingDir());
    }

    private File getWorkingDir() {
        return new File("target");
    }
}
