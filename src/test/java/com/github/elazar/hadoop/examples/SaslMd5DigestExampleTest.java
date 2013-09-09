package com.github.elazar.hadoop.examples;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Show example Sasl Usage
 */
public class SaslMd5DigestExampleTest {

    final String[] digestMd5 = {"DIGEST-MD5"};
    final Map<String, ?> noProps = ImmutableMap.of();

    @Test
    public void testSaslDigestMd5() throws Exception {
        final SaslClient client = Sasl.createSaslClient(
                digestMd5,
                "myAuthorizationId", "protocol", "my_realm", null,
                new CallbackHandler() {
                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        // Provide realm/user/password, requested user name must be authorized to use
                        // the username given to createSaslClient static constructor.
                        for (Callback callback : callbacks) {
                            if (callback instanceof RealmCallback) {
                                // realm of the server is defaulted to my_realm
                                ((RealmCallback) callback).setText("my_realm");
                            } else if (callback instanceof NameCallback) {
                                ((NameCallback) callback).setName("bob");
                            } else if (callback instanceof PasswordCallback) {
                                ((PasswordCallback) callback).setPassword("bob's pass".toCharArray());
                            }
                        }
                    }
                }
        );
        final SaslServer server = Sasl.createSaslServer(
                digestMd5[0], "protocol", "my_realm", noProps,
                new CallbackHandler() {
                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        String user = null;
                        PasswordCallback passwordCb = null;
                        for (Callback callback : callbacks) {
                            // Set required username and password, framework will verify they match
                            if (callback instanceof RealmCallback) {
                                assertThat(((RealmCallback) callback).getDefaultText(),
                                    is(equalTo("my_realm")));
                            } else if (callback instanceof NameCallback) {
                                user = ((NameCallback) callback).getDefaultName();
                            } else if (callback instanceof PasswordCallback) {
                                passwordCb = (PasswordCallback) callback;
                            }
                            if (callback instanceof AuthorizeCallback) {
                                // always allow valid user
                                final AuthorizeCallback cb = (AuthorizeCallback) callback;
                                assertThat(cb.getAuthorizationID(), equalTo("myAuthorizationId"));
                                cb.setAuthorized(true);
                            }
                        }
                        ImmutableMap<String, char[]> usersDb = ImmutableMap.of(
                                "bob", "bob's pass".toCharArray(),
                                "admin", "secret pass".toCharArray()
                        );
                        if (usersDb.containsKey(user) && passwordCb != null) {
                            passwordCb.setPassword(usersDb.get(user));
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

}
