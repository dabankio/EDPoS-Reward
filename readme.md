# dpos奖励计算程序


## install as systemd service

sudo snap install dotnet-sdk --classic

```bash
touch /etc/systemd/system/kestrel-<your_app>.service

sudo systemctl enable kestrel-<app_name>.service
sudo systemctl start kestrel-<app_name>.service 
sudo systemctl status kestrel-alfalab.service
```

```conf
[Unit]
Description=<App Name>

[Service]
WorkingDirectory=/var/www/<app_folder>
ExecStart=/usr/bin/dotnet /var/www/<app_folder>/<app_name>.dll
Restart=always
# Restart service after 10 seconds if the dotnet service crashes:
RestartSec=10
KillSignal=SIGINT
SyslogIdentifier=dotnet-example
User=www-data
Environment=ASPNETCORE_ENVIRONMENT=Production
Environment=DOTNET_PRINT_TELEMETRY_MESSAGE=false

[Install]
WantedBy=multi-user.target 
```
