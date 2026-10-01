# Bridge

The Bridge passes messages between Solace and RabbitMQ in both directions (see `CONTEXT.md` for the terms).

## Run

Build the image:

```
podman build -t vatm-bridge .
```

Export the two passwords in your shell first. The Bridge reads them from the environment variables `SOLACE_PASSWORD` and `RABBITMQ_PASSWORD`. Do not write them in any file.

Start the Bridge:

```
podman run -d --name vatm-bridge --network=host --restart=always -e SOLACE_PASSWORD -e RABBITMQ_PASSWORD vatm-bridge
```

`--network=host` lets the container reach Solace at `localhost:55555`.

`--restart=always` alone does not bring the Bridge back after a reboot. Also enable the Podman restart service:

```
sudo systemctl enable --now podman-restart.service
```

If you run Podman as a normal user (rootless), use this instead:

```
systemctl --user enable --now podman-restart.service
loginctl enable-linger $USER
```

Show the logs:

```
podman logs -f vatm-bridge
```
