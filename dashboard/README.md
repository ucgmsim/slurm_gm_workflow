To run dashboard, please create a ~/.ssh/config file similar to the following and keep the sockets to the 4 hosts open.
By default, dashboard will try log into maui01/mahuika01.
After 3 consecutive unsuccessful ssh command, dashboard will try log into maui02/mahuika02.
After 6 consecutive unsuccessful ssh command, dashboard will exit with error message.
```
Host mahuika
   User melody.zhu
   #HostName login.mahuika.nesi.org.nz
   HostName mahuika01.mahuika.nesi.org.nz
   ProxyCommand ssh -W %h:%p %r@lander02.nesi.org.nz
   ForwardX11 yes
   ForwardX11Trusted yes
   ServerAliveInterval 120

Host maui
   User melody.zhu
  # HostName login.maui.nesi.org.nz
   HostName maui01.maui.niwa.co.nz
   ProxyCommand ssh -W %h:%p %r@lander02.nesi.org.nz
   ForwardX11 yes
   ForwardX11Trusted yes
   ServerAliveInterval 120

Host mahuika02
   User melody.zhu
   HostName mahuika02.mahuika.nesi.org.nz
   ProxyCommand ssh -W %h:%p %r@lander02.nesi.org.nz
   ForwardX11 yes
   ForwardX11Trusted yes
   ServerAliveInterval 120

Host maui02
   User melody.zhu
   HostName maui02.maui.niwa.co.nz
   ProxyCommand ssh -W %h:%p %r@lander02.nesi.org.nz
   ForwardX11 yes
   ForwardX11Trusted yes
   ServerAliveInterval 120

Host *
    ControlMaster auto
    ControlPath ~/.ssh/sockets/ssh_mux_%h_%p_%r
    ControlPersist yes
    ServerAliveInterval 120
```