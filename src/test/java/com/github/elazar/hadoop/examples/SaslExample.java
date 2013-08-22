package com.github.elazar.hadoop.examples;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Show example Sasl Usage
 */
public class SaslExample {

    final String[] digestMd5 = {"DIGEST-MD5"};
    final Map<String, ?> noProps = ImmutableMap.of();

    @Test
    public void testSaslDigestMd5() throws Exception {
        final SaslClient client = Sasl.createSaslClient(
                digestMd5,
                "user", "protocol", "serverName", null,
                new CallbackHandler() {
                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        // Provide realm/user/password, requested user name must be authorized to use
                        // the username given to createSaslClient static constructor.
                        for (Callback callback : callbacks) {
                            if (callback instanceof RealmCallback) {
                                // realm of the server is defaulted to serverName
                                ((RealmCallback) callback).setText("serverName");
                            } else if (callback instanceof NameCallback) {
                                ((NameCallback) callback).setName("user");
                            } else if (callback instanceof PasswordCallback) {
                                ((PasswordCallback) callback).setPassword("pass".toCharArray());
                            }
                        }
                    }
                }
        );
        final SaslServer server = Sasl.createSaslServer(
                digestMd5[0], "protocol", "serverName", noProps,
                new CallbackHandler() {
                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        for (Callback callback : callbacks) {
                            // Set required username and password, framework will verify they match
                            if (callback instanceof RealmCallback) {
                                ((RealmCallback) callback).setText("default");
                            } else if (callback instanceof NameCallback) {
                                ((NameCallback) callback).setName("user");
                            } else if (callback instanceof PasswordCallback) {
                                ((PasswordCallback) callback).setPassword("pass".toCharArray());
                            }
                            // Check if the user is authorized to login
                            if (callback instanceof AuthorizeCallback) {
                                ((AuthorizeCallback) callback).setAuthorized(true);
                            }
                        }
                    }
                }
        );
        byte[] resp = new byte[]{};
        assertThat(client.hasInitialResponse(), is(false));
        // in principle there should be a loop here, but we know
        // that this MD5-digest implementation is done after 3 rounds
        resp = server.evaluateResponse(resp);
        resp = client.evaluateChallenge(resp);
        resp = server.evaluateResponse(resp);
        assertThat(server.isComplete(), is(true));
        assertThat(client.evaluateChallenge(resp), is(nullValue()));
        assertThat(client.isComplete(), is(true));
    }

    @Test
    public void testSaslGssapi() throws Exception {
        Sasl.createSaslClient(
                new String[]{"GSSAPI", "GSSAPI"},
                null, null, null, null, null
        );

    }

}
