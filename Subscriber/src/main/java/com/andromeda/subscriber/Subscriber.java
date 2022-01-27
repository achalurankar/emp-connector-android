package com.andromeda.subscriber;

import android.util.Log;

import com.andromeda.subscriber.lib.BayeuxParameters;
import com.andromeda.subscriber.lib.EmpConnector;
import com.andromeda.subscriber.lib.LoginHelper;
import com.andromeda.subscriber.lib.TopicSubscription;

import org.eclipse.jetty.util.ajax.JSON;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Subscriber {
    private static final ExecutorService workerThreadPool = Executors.newFixedThreadPool(1);

    public interface ResponseListener {
        // on event received
        void onEvent(String response);

        // on error occurred
        void onError(String error);
    }

    public static void subscribe(String username, String password, String eventName, ResponseListener responseListener) {
        new Thread(() -> {
            long replayFrom = EmpConnector.REPLAY_FROM_TIP;
            BayeuxParameters params;
            try {
                BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
                    try {
                        return LoginHelper.login(username, password);
                    } catch (Exception e) {
                        responseListener.onError(e.getMessage());
                        Log.e("Subscriber", "subscribe: exception thrown in tokenProvider");
                        e.printStackTrace();
                        return null;
                    }
                });
                params = tokenProvider.login();
                Consumer<Map<String, Object>> consumer = event -> workerThreadPool.submit(() -> responseListener.onEvent(JSON.toString(event)));
                EmpConnector connector = new EmpConnector(params);
                connector.setBearerTokenProvider(tokenProvider);
                connector.start().get(5, TimeUnit.SECONDS);
                TopicSubscription subscription = connector.subscribe(eventName, replayFrom, consumer).get(5, TimeUnit.SECONDS);
                responseListener.onEvent(String.format("Subscribed: %s", subscription));
            } catch (Exception e) {
                e.printStackTrace();
                Log.e("Subscriber", "subscribe: exception thrown");
                responseListener.onError(e.getMessage());
            }
        }).start();
    }
}
