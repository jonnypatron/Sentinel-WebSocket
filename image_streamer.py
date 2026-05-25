#!/usr/bin/env python3
"""
image_streamer.py — ROS2 WebSocket Image Streamer (Python)
Tradução de imageStreamer.cpp (C++ + websocketpp) para Python (rclpy + websockets).

Protocolo binário idêntico ao original (compatível com imageFeedService.js):
  [4B uint32 LE = headerSize][JSON header: {topic, format, timestamp}][bytes da imagem]

Dependência extra (além do ROS2):
  pip3 install websockets
"""

import asyncio
import json
import struct
import sys
import threading

import rclpy
import rclpy.executors
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    QoSReliabilityPolicy,
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
)
from sensor_msgs.msg import CompressedImage

import websockets
import websockets.exceptions

# ── Cores ANSI para o terminal ─────────────────────────────────────────────────
GREEN  = "\033[1;32m"
RED    = "\033[1;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[1;36m"
CLEAR  = "\033[0m"


# ── Estruturas de dados ────────────────────────────────────────────────────────
# Equivalentes a ClientState e TopicState do C++.
# Guardados em dicts simples; protegidos por _lock.
#
# _client_states : { ws → {"id": int, "active_topic": str} }
# _topic_states  : { topic_str → {"subscription": rclpy.Subscription,
#                                  "clients": set[ws]} }


class ImageStreamer(Node):
    """
    Nó ROS2 que subscreve tópicos CompressedImage e os transmite via WebSocket.
    O servidor WebSocket corre num thread separado com asyncio.
    """

    def __init__(self):
        super().__init__("gui_image_streamer")

        # ── Parâmetro ROS2: porta do servidor ──────────────────────────────────
        self.declare_parameter("socket_port", 9092)
        self._port: int = (
            self.get_parameter("socket_port").get_parameter_value().integer_value
        )

        # Argumento CLI sobrepõe o parâmetro ROS2 (comportamento igual ao C++)
        if len(sys.argv) >= 2:
            try:
                self._port = int(sys.argv[1])
            except ValueError:
                self.get_logger().warn(
                    f"{YELLOW}[ImageStreamer] Porta CLI inválida '{sys.argv[1]}'"
                    f", a usar {self._port}{CLEAR}"
                )

        # ── Estado partilhado (threads ROS + asyncio) ──────────────────────────
        self._lock = threading.Lock()
        self._next_id: int = 1
        self._client_states: dict = {}   # ws → {"id", "active_topic"}
        self._topic_states: dict  = {}   # topic → {"subscription", "clients"}

        # ── Event loop asyncio num thread dedicado ────────────────────────────
        self._loop = asyncio.new_event_loop()
        self._ws_thread = threading.Thread(
            target=self._run_ws_server, daemon=True, name="ws_thread"
        )
        self._ws_thread.start()

        self.get_logger().info(
            f"{CYAN}[ImageStreamer] A iniciar na porta {self._port}{CLEAR}"
        )

    # ── WebSocket server ───────────────────────────────────────────────────────

    def _run_ws_server(self):
        """Corre o event loop asyncio com o servidor WebSocket (thread separado)."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve())

    async def _serve(self):
        """Abre o servidor e fica à escuta para sempre."""
        async with websockets.serve(self._handle_client, "0.0.0.0", self._port):
            self.get_logger().info(
                f"{GREEN}[ImageStreamer] WebSocket a escutar na porta {self._port}{CLEAR}"
            )
            await asyncio.Future()  # bloqueia até o nó ser destruído

    async def _handle_client(self, ws):
        """Callback chamado para cada nova ligação WebSocket."""
        # Regista o cliente (equivalente a wsServer.set_open_handler no C++)
        with self._lock:
            client_id = self._next_id
            self._next_id += 1
            self._client_states[ws] = {"id": client_id, "active_topic": ""}
            total = len(self._client_states)

        self.get_logger().info(
            f"{CYAN}[ImageStreamer] Cliente {client_id} ligado. Total: {total}{CLEAR}"
        )

        try:
            # Loop de receção de mensagens
            async for message in ws:
                if isinstance(message, str):
                    await self._handle_message(ws, message)
                # Mensagens binárias ignoradas — o cliente não envia binário
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            # Limpeza ao desligar (equivalente a wsServer.set_close_handler)
            await self._cleanup_client(ws)

    async def _handle_message(self, ws, payload: str):
        """
        Processa um comando JSON enviado pelo cliente.
        Protocolo (texto):
          { "cmd": "enable",  "topic": "/camera/compressed" }
          { "cmd": "disable", "topic": "/camera/compressed" }
          { "cmd": "switch",  "topic": "/camera2/compressed" }
        """
        if len(payload) < 2:
            return

        try:
            j = json.loads(payload)
        except json.JSONDecodeError:
            self.get_logger().error(
                f"{RED}[ImageStreamer] Erro a parsear JSON: {payload[:80]}{CLEAR}"
            )
            return

        cmd   = j.get("cmd",   "")
        topic = j.get("topic", "")

        if not isinstance(cmd, str) or not isinstance(topic, str):
            return
        if not topic or not topic.startswith("/"):
            return

        # Despacha para o handler correto (mesmo que messageHandler() no C++)
        if   cmd == "enable":  self._handle_enable(ws, topic)
        elif cmd == "disable": self._handle_disable(ws, topic)
        elif cmd == "switch":  self._handle_switch(ws, topic)

    async def _cleanup_client(self, ws):
        """
        Remove o cliente dos estados e cancela a subscrição ROS2 se ficar vazia.
        Equivalente a wsServer.set_close_handler no C++.
        """
        sub_to_destroy = None
        client_id      = -1
        active_topic   = ""
        remaining      = 0
        total          = 0

        with self._lock:
            state = self._client_states.pop(ws, None)
            if state:
                client_id    = state["id"]
                active_topic = state["active_topic"]
            total = len(self._client_states)

            if active_topic:
                ts = self._topic_states.get(active_topic)
                if ts:
                    ts["clients"].discard(ws)
                    remaining = len(ts["clients"])
                    if not ts["clients"]:
                        sub_to_destroy = ts["subscription"]
                        del self._topic_states[active_topic]

        # destroy_subscription deve ser chamado fora do lock
        if sub_to_destroy:
            self._destroy_sub(sub_to_destroy)

        if active_topic:
            self.get_logger().info(
                f"{CYAN}[ImageStreamer] Cliente {client_id} desligado de "
                f"'{active_topic}'. Restantes: {remaining}. Total: {total}{CLEAR}"
            )
        else:
            self.get_logger().info(
                f"{CYAN}[ImageStreamer] Cliente {client_id} desligado. Total: {total}{CLEAR}"
            )

    # ── Comandos de stream ────────────────────────────────────────────────────
    # Equivalentes a handleStreamEnable/Disable/Switch no C++.
    # Chamados a partir do thread asyncio mas protegidos pelo _lock.

    def _handle_enable(self, ws, topic: str):
        """Começa a enviar o tópico para este cliente."""
        sub_created = None
        client_id   = -1
        viewers     = 0

        with self._lock:
            state = self._client_states.get(ws)
            if not state:
                return
            client_id = state["id"]

            if state["active_topic"]:
                self.get_logger().warn(
                    f"{YELLOW}[ImageStreamer] Cliente {client_id} já está a ver "
                    f"'{state['active_topic']}'. Usa 'switch'.{CLEAR}"
                )
                return

            # Cria subscrição ROS2 se ainda não existir para este tópico
            if topic not in self._topic_states:
                sub = self._create_sub(topic)
                self._topic_states[topic] = {"subscription": sub, "clients": set()}

            self._topic_states[topic]["clients"].add(ws)
            state["active_topic"] = topic
            viewers = len(self._topic_states[topic]["clients"])

        self.get_logger().info(
            f"{GREEN}[ImageStreamer] Cliente {client_id} -> '{topic}'. "
            f"Viewers: {viewers}{CLEAR}"
        )

    def _handle_disable(self, ws, topic: str):
        """Para de enviar o tópico para este cliente."""
        sub_to_destroy = None
        client_id      = -1
        viewers        = 0

        with self._lock:
            state = self._client_states.get(ws)
            if not state or state["active_topic"] != topic:
                return
            client_id = state["id"]

            ts = self._topic_states.get(topic)
            if ts:
                ts["clients"].discard(ws)
                viewers = len(ts["clients"])
                if not ts["clients"]:
                    sub_to_destroy = ts["subscription"]
                    del self._topic_states[topic]

            state["active_topic"] = ""

        if sub_to_destroy:
            self._destroy_sub(sub_to_destroy)

        self.get_logger().info(
            f"{GREEN}[ImageStreamer] Cliente {client_id} -> Disabled "
            f"'{topic}'. Viewers: {viewers}{CLEAR}"
        )

    def _handle_switch(self, ws, new_topic: str):
        """Troca atomicamente de tópico (sem reconexão WebSocket)."""
        old_sub_to_destroy = None
        client_id          = -1
        old_topic          = ""
        new_viewers        = 0
        old_viewers        = 0

        with self._lock:
            state = self._client_states.get(ws)
            if not state:
                return
            client_id = state["id"]
            old_topic = state["active_topic"]

            if old_topic == new_topic:
                return  # já está no tópico pedido

            # Subscreve o novo tópico se ainda não existir
            if new_topic not in self._topic_states:
                sub = self._create_sub(new_topic)
                self._topic_states[new_topic] = {"subscription": sub, "clients": set()}

            self._topic_states[new_topic]["clients"].add(ws)
            new_viewers = len(self._topic_states[new_topic]["clients"])
            state["active_topic"] = new_topic

            # Remove do tópico antigo
            if old_topic:
                ts = self._topic_states.get(old_topic)
                if ts:
                    ts["clients"].discard(ws)
                    old_viewers = len(ts["clients"])
                    if not ts["clients"]:
                        old_sub_to_destroy = ts["subscription"]
                        del self._topic_states[old_topic]

        if old_sub_to_destroy:
            self._destroy_sub(old_sub_to_destroy)

        self.get_logger().info(
            f"{GREEN}[ImageStreamer] Cliente {client_id} -> "
            f"'{'none' if not old_topic else old_topic}' → '{new_topic}'. "
            f"Viewers novo: {new_viewers}, antigo: {old_viewers}{CLEAR}"
        )

    # ── Helpers de subscrição ROS2 ────────────────────────────────────────────

    def _create_sub(self, topic: str):
        """
        Cria uma subscrição ROS2 para CompressedImage.
        QoS equivalente a SensorDataQoS do C++: best_effort, volatile, depth=1.
        """
        qos = QoSProfile(
            reliability=QoSReliabilityPolicy.BEST_EFFORT,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1,
        )
        return self.create_subscription(
            CompressedImage,
            topic,
            lambda msg, t=topic: self._image_callback(msg, t),
            qos,
        )

    def _destroy_sub(self, sub) -> None:
        """Destrói uma subscrição ROS2 de forma segura."""
        try:
            self.destroy_subscription(sub)
        except Exception as e:
            self.get_logger().warn(
                f"{YELLOW}[ImageStreamer] Erro ao destruir subscrição: {e}{CLEAR}"
            )

    # ── Image callback (thread do executor ROS2) ──────────────────────────────

    def _image_callback(self, msg: CompressedImage, topic: str) -> None:
        """
        Chamado pelo executor ROS2 a cada nova frame.
        Constrói o frame binário e envia para todos os clientes interessados.

        Frame layout (idêntico ao C++):
          [4B uint32 LE headerSize][JSON header bytes][bytes da imagem]
        """
        # Recolhe clientes interessados neste tópico
        with self._lock:
            ts = self._topic_states.get(topic)
            if not ts or not ts["clients"]:
                return
            interested = list(ts["clients"])

        # Constrói o header JSON (equivalente ao nlohmann::json no C++)
        header = {
            "topic":     topic,
            "format":    msg.format,
            "timestamp": msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9,
        }
        header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
        # uint32 little-endian (equivalente a memcpy de headerSize no C++)
        prefix = struct.pack("<I", len(header_bytes))
        frame  = prefix + header_bytes + bytes(msg.data)

        # Agenda o envio no loop asyncio (thread-safe: run_coroutine_threadsafe)
        for ws in interested:
            asyncio.run_coroutine_threadsafe(
                self._send_frame(ws, frame), self._loop
            )

    async def _send_frame(self, ws, frame: bytes) -> None:
        """Envia um frame binário para um cliente. Corre no loop asyncio."""
        try:
            await ws.send(frame)
        except (websockets.exceptions.ConnectionClosed, Exception) as e:
            # Cliente desligou entretanto — ignorado silenciosamente
            with self._lock:
                state = self._client_states.get(ws)
                cid   = state["id"] if state else "?"
            self.get_logger().error(
                f"{RED}[ImageStreamer] Falha ao enviar para cliente {cid}: {e}{CLEAR}"
            )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("[ImageStreamer] A iniciar...", flush=True)
    rclpy.init()
    node = ImageStreamer()

    # MultiThreadedExecutor com 2 threads (equivalente ao C++)
    executor = rclpy.executors.MultiThreadedExecutor(num_threads=2)
    executor.add_node(node)

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()