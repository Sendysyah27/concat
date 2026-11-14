// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 Jareer Abdullah and Concat contributors

package app.concat.editor;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Intent;
import android.os.Build;
import android.os.IBinder;
import android.util.Log;

/**
 * The foreground service that keeps the process alive while the API
 * server runs. A phone's system freezes a process it has cached, and a
 * frozen server answers nobody - not even its own auth - which is the
 * timeout the automation's caller sees. A foreground service, with its
 * notification, is not frozen, so the server keeps answering while it
 * runs. Started and stopped by the automation's switch, through
 * {@link ConcatFiles#startServerService} and
 * {@link ConcatFiles#stopServerService}; in the app's own process, so
 * the server it keeps alive is the one the activity started.
 */
public class ConcatServerService extends Service {
    private static final String TAG = "ConcatServerService";
    private static final int NOTIFICATION_ID = 0xC0C6;
    private static final String CHANNEL_ID = "concat-server";

    @Override
    public void onCreate() {
        super.onCreate();
        NotificationManager manager =
                (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (manager != null && Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID,
                    "Automation server",
                    NotificationManager.IMPORTANCE_LOW);
            channel.setDescription("Keeps the automation server answering while it runs");
            manager.createNotificationChannel(channel);
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Notification.Builder builder = new Notification.Builder(this)
                .setContentTitle("Concat")
                .setContentText("Automation server is running")
                .setSmallIcon(android.R.drawable.ic_dialog_info)
                .setOngoing(true);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            builder.setChannelId(CHANNEL_ID);
        }
        Notification notification = builder.build();
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(
                        NOTIFICATION_ID,
                        notification,
                        android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE);
            } else {
                startForeground(NOTIFICATION_ID, notification);
            }
        } catch (RuntimeException e) {
            Log.w(TAG, "could not go to the foreground", e);
        }
        // If the system kills it, the server's own watchdog - or the next
        // launch - brings a fresh one; a stale one coming back on its own
        // would serve an old token to nobody in particular.
        return START_NOT_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }
}
