from paho.mqtt import client as mqtt_client

class MQTTVersion(enumerate):
    MQTTv31 = mqtt_client.MQTTv31
    MQTTv311 = mqtt_client.MQTTv311
    MQTTv5 = mqtt_client.MQTTv5

class MQTTTransport(enumerate):
    TCP = 'tcp'
    WEB_SOCKET = 'websockets'

"""
封裝 MQTT 功能的 Class
"""
class MQTT:

    _mqtt_client: mqtt_client.Client = None
    _is_connected: bool = False
    _on_connect: callable = None
    _on_disconnect: callable = None
    _on_message: callable = None
    _on_messages: dict[str, callable] = {}

    # MQTT Settings (MQTT 連線設定)
    _mqtt_settings_broker: str = None
    _mqtt_settings_port: int = None
    _mqtt_settings_client_id: str = None
    _mqtt_settings_version: MQTTVersion = None
    _mqtt_settings_topics: dict[str, int] = {}
    _mqtt_settings_auto_reconnect: bool = True
    
    def __init__(self, client_id: str, host: str, port: int, version: MQTTVersion = MQTTVersion.MQTTv311):
        """
        初始化 MQTT Broker 設定

        Args:
            client_id (str): Client ID
            host (str): Broker Host
            port (int): Broker Port
            version (MQTTVersion, optional): MQTT Version. Defaults to MQTTVersion.MQTTv311.
        """

        self._mqtt_settings_client_id = client_id
        self._mqtt_settings_broker = host
        self._mqtt_settings_port = port
        self._mqtt_settings_version = version

        if version == MQTTVersion.MQTTv5:
            self._mqtt_client = mqtt_client.Client(
                client_id=self._mqtt_settings_client_id, 
                transport=MQTTTransport.TCP, 
                protocol=version
            )
        else:
            self._mqtt_client = mqtt_client.Client(
                client_id=self._mqtt_settings_client_id, 
                transport=MQTTTransport.TCP, 
                protocol=version,
                clean_session=True
            )

    def connect(self, username: str, password: str, use_ssl: bool = True, auto_reconnect: bool = True):
        """
        連線到 MQTT Broker
        
        Args:
            username (str): 驗證帳號
            password (str): 驗證密碼
            use_ssl (bool, optional): 是否使用 SSL 連線. Defaults to True.
            auto_reconnect (bool, optional): 是否自動重新連線. Defaults to True.
        """

        self._mqtt_settings_auto_reconnect = auto_reconnect
        self._mqtt_client.username_pw_set(username, password)
    
        if use_ssl:
            self._mqtt_client.tls_set()

        # 註冊 Callback
        self._mqtt_client.on_connect = self._on_connect_callback
        self._mqtt_client.on_disconnect = self._on_disconnect_callback
        self._mqtt_client.on_message = self._on_message_callback
        #self._mqtt_client.on_publish = self._on_publish_callback
        #self._mqtt_client.on_subscribe = self._on_subscribe_callback
        #self._mqtt_client.on_unsubscribe = self._on_unsubscribe_callback

        if self._mqtt_settings_version == MQTTVersion.MQTTv5:
            from paho.mqtt.properties import Properties
            from paho.mqtt.packettypes import PacketTypes
            properties = Properties(PacketTypes.CONNECT)
            # 30 分鐘
            properties.SessionExpiryInterval = 1800 
            
            self._mqtt_client.connect(
                host=self._mqtt_settings_broker,
                port=self._mqtt_settings_port,
                clean_start=mqtt_client.MQTT_CLEAN_START_FIRST_ONLY,
                properties=properties,
                keepalive=60
            )
        else:
            self._mqtt_client.connect(
                host=self._mqtt_settings_broker,
                port=self._mqtt_settings_port,
                keepalive=60
            )

        self._mqtt_client.loop_forever()

    def disconnect(self):
        """
        中斷 MQTT 連線
        """
        self._mqtt_settings_auto_reconnect = False
        self._mqtt_client.disconnect()

    def subscribeTopic(self, topic: str, qos: int, callback: callable = None) -> bool:
        """
        訂閱 Topic

        Args:
            topic (str): 訂閱的 Topic

        Returns:
            bool: 是否訂閱成功
        """
        self._mqtt_settings_topics[topic] = qos
        if callback != None:
            self._on_messages[topic] = callback

    # 內部函式，處理連線的 Callback
    def _on_connect_callback(self, client, userdata, flags, rc, properties = None):

        self._is_connected = rc == 0

        print(self._is_connected)

        if rc == 0 and len(self._mqtt_settings_topics) > 0:
            for key, value in self._mqtt_settings_topics.items():
                self._mqtt_client.subscribe(topic=key, qos=value)

        if self._on_connect:
            self._on_connect(rc)

    
    # 內部函式，處理中斷連線的 Callback
    def _on_disconnect_callback(self, client, userdata, rc):
        self._is_connected = False

        if self._on_disconnect:
            self._on_disconnect(rc)

        # 重新連線
        if self._mqtt_settings_auto_reconnect:
            client.reconnect()

    # 內部函式，收到訊息時觸發的 Callback
    def _on_message_callback(self, client, userdata, message, tmp = None):

        topic = message.topic
        msg = message.payload.decode('utf-8')

        if self._on_message:
            self._on_message(topic, msg)

        if message.topic in self._on_messages:
            self._on_messages[message.topic](msg)