from flask import Flask, render_template, redirect, url_for, session, request
from flask_socketio import SocketIO
from meshtastic.serial_interface import SerialInterface
from pubsub import pub
from datetime import datetime
import threading

# ================= Flask & SocketIO =================
app = Flask(__name__)
app.secret_key = "super_secret_key"

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ================= Data =================
messages = []
sensor_nodes = {}

sensor_data = {
    "temperature": "--",
    "humidity": "--",
    "pressure": "--",
    "gas": "--",
    "time": "--"
}

meshtastic_started = False

# ================= Utils =================
def safe_round(value, digits=2):
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return value

# ================= Meshtastic Callback =================
def on_receive(packet, interface):
    global sensor_data

    try:
        decoded = packet.get("decoded")
        if not decoded:
            return

        portnum = decoded.get("portnum")
        sender = packet.get("fromId", "Unknown")
        now = datetime.now().strftime("%H:%M:%S")

        # =================== CAPTEURS ===================
        if portnum == "TELEMETRY_APP":
            telemetry = decoded.get("telemetry", {})
            env = telemetry.get("environmentMetrics", {})

            previous = sensor_nodes.get(sender, sensor_data.copy())

            node_data = {
                "temperature": safe_round(env.get("temperature", previous["temperature"])),
                "humidity": safe_round(env.get("relativeHumidity", previous["humidity"])),
                "pressure": safe_round(env.get("barometricPressure", previous["pressure"])),
                "gas": safe_round(env.get("gasResistance", previous["gas"])),
                "time": now
            }

            sensor_nodes[sender] = node_data
            sensor_data = node_data

            socketio.emit("sensor_update", {
                "node": sender,
                "data": node_data
            })

            print(f"🌡 Capteurs reçus de {sender} :", node_data)
            return

        # =================== MESSAGES ===================
        if portnum == "TEXT_MESSAGE_APP":
            text = decoded.get("text", "")
            if not text:
                return

            msg = {
                "sender": sender,
                "text": text,
                "time": now
            }

            messages.append(msg)
            socketio.emit("new_message", msg)

            print(f"📩 Message : {sender} → {text}")

    except Exception as e:
        print("❌ Erreur réception Meshtastic :", e)

# ================= Démarrage Meshtastic =================
def start_meshtastic():
    global meshtastic_started
    if meshtastic_started:
        return

    meshtastic_started = True

    try:
        SerialInterface(devPath="COM14")
        pub.subscribe(on_receive, "meshtastic.receive")
        print("✅ Meshtastic connecté")
    except Exception as e:
        print("❌ Connexion Meshtastic échouée :", e)

threading.Thread(target=start_meshtastic, daemon=True).start()

# ================= Routes =================
@app.route("/")
def index():
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form["username"] == "hajer" and request.form["password"] == "password":
            session["logged_in"] = True
            return redirect(url_for("capteurs"))
    return render_template("login.html")

@app.route("/messagerie")
def messagerie():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template("messagerie.html", messages=messages)

@app.route("/capteurs")
def capteurs():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template(
        "capteurs.html",
        sensor_nodes=sensor_nodes,
        sensor_data=sensor_data
    )

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ================= SocketIO =================
@socketio.on("connect")
def connect():
    socketio.emit("init_sensors", {
        "nodes": sensor_nodes,
        "main": sensor_data
    }, to=request.sid)

# ================= Run =================
if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=True,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    )
