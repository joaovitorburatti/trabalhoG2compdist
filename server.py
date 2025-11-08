import asyncio
import os
from aiohttp import web
import json
from rabbit_bridge import RabbitBridge  # comunicação com RabbitMQ

HOST = "0.0.0.0"
PORT = 5000
HIST_FILE = "historico_chat.txt"
connected = set()

# -----------------------
# RabbitMQ (CloudAMQP)
# -----------------------
rabbit = RabbitBridge("amqps://yzzfroga:cA8xnQgjhmkwU1Cq-w6rCsEteyLi6jqe@jaragua.lmq.cloudamqp.com/yzzfroga")

# -----------------------
# Histórico
# -----------------------
def append_history(line):
    with open(HIST_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


async def send_history(ws):
    if not os.path.exists(HIST_FILE):
        await ws.send_json({"type": "historic", "lines": []})
        return

    with open(HIST_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f.readlines()]

    await ws.send_json({"type": "historic", "lines": lines[-100:]})


# -----------------------
# RabbitMQ - integração
# -----------------------
async def rabbit_callback(message):
    """Recebe mensagens publicadas por outras instâncias e envia aos clientes conectados."""
    try:
        data = json.loads(message)
        tipo = data.get("type")

        if tipo == "message":
            print(f"[RABBIT] Mensagem recebida -> {data['from']}: {data['text']}")
            await broadcast_message(data["from"], data["text"])

        elif tipo == "system":
            print(f"[RABBIT] Sistema -> {data['text']}")
            await broadcast_system(data["text"])

    except Exception as e:
        print("Erro ao processar mensagem do Rabbit:", e)


async def setup_rabbit():
    """Inicia o consumo das mensagens do RabbitMQ."""
    await rabbit.connect()
    await rabbit.consume("chat_queue", rabbit_callback)


# -----------------------
# Broadcast
# -----------------------
async def broadcast_message(nome, texto, sender_ws=None):
    data = {"type": "message", "from": nome, "text": texto}
    for c in list(connected):
        try:
            await c.send_json(data)
        except:
            connected.discard(c)


async def broadcast_system(texto):
    data = {"type": "system", "text": texto}
    for c in list(connected):
        try:
            await c.send_json(data)
        except:
            connected.discard(c)


# -----------------------
# WebSocket handler
# -----------------------
async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    username = None
    connected.add(ws)

    async for msg in ws:
        if msg.type != web.WSMsgType.TEXT:
            continue

        data = json.loads(msg.data)

        # Cliente informa o nome
        if data.get("type") == "join":
            username = data.get("user")
            await ws.send_json({"type": "join_ok"})
            print(f"[JOIN] {username} entrou no chat.")
            await rabbit.publish("chat_queue", json.dumps({"type": "system", "text": f"{username} entrou no chat."}))
            continue

        # Histórico
        if data.get("type") == "historic_request":
            await send_history(ws)
            continue

        # Mensagem normal
        if data.get("type") == "message":
            texto = data.get("text")
            if username:
                append_history(f"{username}: {texto}")
                print(f"[LOCAL] {username}: {texto}")
                await rabbit.publish("chat_queue", json.dumps({"type": "message", "from": username, "text": texto}))
            continue

    if username:
        print(f"[EXIT] {username} saiu do chat.")
        await rabbit.publish("chat_queue", json.dumps({"type": "system", "text": f"{username} saiu do chat."}))

    if ws in connected:
        connected.remove(ws)

    return ws


# -----------------------
# Servir index.html
# -----------------------
async def index(request):
    path = os.path.join(os.path.dirname(__file__), "index.html")
    return web.FileResponse(path)


# -----------------------
# Inicialização do app
# -----------------------
app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/ws", ws_handler)


async def on_startup(app):
    await setup_rabbit()


app.on_startup.append(on_startup)


if __name__ == "__main__":
    print(f"Servidor ativo em http://{HOST}:{PORT}")
    web.run_app(app, host=HOST, port=PORT)
