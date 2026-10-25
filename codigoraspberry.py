import socket
import select
import json
import csv
import time
import math
import threading
import queue
from pathlib import Path
from datetime import datetime
from collections import deque

import tkinter as tk
from tkinter import ttk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ========================================
# CONFIGURACION DEL SERVIDOR
# ========================================

HOST = "0.0.0.0"
PUERTO = 5000

CARPETA = Path.home() / "Documents" / "tic3" / "sensores"
CARPETA.mkdir(parents=True, exist_ok=True)

COLUMNAS = [
    "fecha_hora", "protocolo", "seq", "t_ms",
    "acc_x_ms2", "acc_y_ms2", "acc_z_ms2",
    "gyr_x_rads", "gyr_y_rads", "gyr_z_rads",
    "odr_hz", "rango_g", "periodo_ms",
    "modo", "frecuencia_hz", "cantidad_configurada"
]

# Tamaño de lote compatible tanto con el firmware original del ESP32 (1..10000) como con el nuevo
LOTE_CONTINUO = 10000

# Comunicacion entre GUI y servidor
ordenes = queue.Queue()
eventos = queue.Queue()

# ========================================
# SERVIDOR TCP + UDP
# ========================================

def emitir(tipo, valor=None):
    eventos.put((tipo, valor))


def procesar_linea_imu(texto, estado_sesion):
    """Procesa un paquete JSON de la IMU recibido por TCP o UDP."""
    try:
        dato = json.loads(texto)

        if dato.get("type") != "imu":
            return

        if not estado_sesion["activa"] or estado_sesion["escritor"] is None:
            return

        if estado_sesion["tiempo_cumplido"]:
            return

        f, r, n, m, proto, duracion_s, es_continuo = estado_sesion["config"]
        ahora_pc = time.perf_counter()

        seq_raw = int(dato["seq"])
        seq = estado_sesion["seq_offset"] + seq_raw
        t_ms = int(dato["t_ms"])

        if estado_sesion["t_ms_inicial"] is None:
            estado_sesion["t_ms_inicial"] = t_ms

        t_rel_sensor = max(0.0, (t_ms - estado_sesion["t_ms_inicial"]) / 1000.0)

        # Metricas para comparacion TCP vs UDP
        if estado_sesion["t_inicio_pc"] is None:
            estado_sesion["t_inicio_pc"] = ahora_pc
        estado_sesion["t_ultimo_pc"] = ahora_pc

        t_rel_pc = max(0.0, ahora_pc - estado_sesion["t_inicio_pc"])

        # En modo Por Tiempo (s), usar el tiempo real transcurrido desde la primera muestra
        if duracion_s is not None and (t_rel_sensor >= duracion_s or t_rel_pc >= duracion_s):
            estado_sesion["tiempo_cumplido"] = True
            t_rel_mostrar = float(duracion_s)
        else:
            t_rel_mostrar = t_rel_sensor if t_rel_sensor > 0.0 else t_rel_pc

        if estado_sesion["ultimo_arribo_pc"] is not None:
            dt_ms = (ahora_pc - estado_sesion["ultimo_arribo_pc"]) * 1000.0
            estado_sesion["intervalos_ms"].append(dt_ms)
        estado_sesion["ultimo_arribo_pc"] = ahora_pc

        if estado_sesion["ultimo_seq"] is not None:
            if seq < estado_sesion["ultimo_seq"]:
                estado_sesion["fuera_de_orden"] += 1
            elif seq > estado_sesion["ultimo_seq"] + 1:
                estado_sesion["perdidos_seq"] += (seq - estado_sesion["ultimo_seq"] - 1)
        estado_sesion["ultimo_seq"] = max(estado_sesion["ultimo_seq"] or 0, seq)
        estado_sesion["bytes_recibidos"] += len(texto.encode("utf-8")) + 1

        cantidad_cfg_csv = "INF" if es_continuo else (f"{duracion_s}s" if duracion_s is not None else n)

        fila = {
            "fecha_hora": datetime.now().isoformat(timespec="milliseconds"),
            "protocolo": dato.get("proto", proto),
            "seq": seq,
            "t_ms": t_ms,
            "acc_x_ms2": float(dato["acc_x_ms2"]),
            "acc_y_ms2": float(dato["acc_y_ms2"]),
            "acc_z_ms2": float(dato["acc_z_ms2"]),
            "gyr_x_rads": float(dato["gyr_x_rads"]),
            "gyr_y_rads": float(dato["gyr_y_rads"]),
            "gyr_z_rads": float(dato["gyr_z_rads"]),
            "odr_hz": dato.get("odr_hz", ""),
            "rango_g": dato.get("rango_g", ""),
            "periodo_ms": dato.get("periodo_ms", ""),
            "modo": m,
            "frecuencia_hz": f,
            "cantidad_configurada": cantidad_cfg_csv
        }

        estado_sesion["escritor"].writerow(fila)
        estado_sesion["archivo"].flush()
        estado_sesion["guardadas"] += 1

        fila_gui = dict(fila)
        fila_gui["t_rel"] = t_rel_mostrar
        emitir("dato", fila_gui)

    except (ValueError, KeyError, TypeError) as error:
        emitir("error", str(error))


def calcular_resumen_protocolo(estado_sesion, texto_fin, cortado_por_stop=False):
    """Calcula las metricas finales de la sesion para comparar TCP vs UDP."""
    guardadas = estado_sesion["guardadas"]
    f, r, n, m, proto, duracion_s, es_continuo = estado_sesion["config"]

    enviadas_esp = guardadas
    if not cortado_por_stop and texto_fin.startswith("END,"):
        partes = texto_fin.split(",")
        if len(partes) >= 2 and partes[1].strip().isdigit():
            enviadas_esp = estado_sesion["enviadas_acumuladas"] + int(partes[1].strip())

    if estado_sesion["ultimo_seq"] is not None:
        if cortado_por_stop:
            # Si fue detenido por tiempo o boton STOP, contar hasta el ultimo numero de secuencia recibido
            enviadas_esp = max(guardadas, estado_sesion["ultimo_seq"] + 1)
        else:
            enviadas_esp = max(enviadas_esp, estado_sesion["ultimo_seq"] + 1)

    perdidos = max(0, enviadas_esp - guardadas)
    perdida_pct = (perdidos / enviadas_esp * 100.0) if enviadas_esp > 0 else 0.0

    if estado_sesion["t_inicio_pc"] and estado_sesion["t_ultimo_pc"]:
        duracion_real = max(0.001, estado_sesion["t_ultimo_pc"] - estado_sesion["t_inicio_pc"])
    else:
        duracion_real = 0.001

    if duracion_s is not None and estado_sesion["tiempo_cumplido"]:
        duracion_real = float(duracion_s)

    frec_efectiva = guardadas / duracion_real if guardadas > 0 else 0.0
    throughput_kbs = (estado_sesion["bytes_recibidos"] / 1024.0) / duracion_real if guardadas > 0 else 0.0

    intervalos = estado_sesion["intervalos_ms"]
    if intervalos:
        dt_prom = sum(intervalos) / len(intervalos)
        varianza = sum((x - dt_prom) ** 2 for x in intervalos) / len(intervalos)
        jitter_ms = math.sqrt(varianza)
    else:
        dt_prom = 0.0
        jitter_ms = 0.0

    return {
        "protocolo": proto,
        "modo": m,
        "frecuencia_cfg": f,
        "enviadas": enviadas_esp,
        "recibidas": guardadas,
        "perdidos": perdidos,
        "perdida_pct": perdida_pct,
        "fuera_orden": estado_sesion["fuera_de_orden"],
        "frec_efectiva_hz": frec_efectiva,
        "dt_prom_ms": dt_prom,
        "jitter_ms": jitter_ms,
        "throughput_kbs": throughput_kbs,
        "duracion_s": duracion_real
    }


def cerrar_sesion_activa(estado_sesion, mensaje_fin, cortado_por_stop=False):
    """Cierra el archivo CSV y emite el resumen final a la interfaz."""
    resumen = calcular_resumen_protocolo(
        estado_sesion, mensaje_fin, cortado_por_stop=cortado_por_stop
    )

    if estado_sesion["archivo"] is not None:
        estado_sesion["archivo"].close()
        estado_sesion["archivo"] = None
        estado_sesion["escritor"] = None

    estado_sesion["activa"] = False

    emitir("fin", {
        "mensaje": mensaje_fin,
        "guardadas": estado_sesion["guardadas"],
        "resumen": resumen
    })


def detener_esp32_y_cerrar(conexion, servidor_udp, estado_sesion, mensaje_motivo):
    """
    Detiene la sesion inmediatamente en la Raspberry Pi y envia STOP al ESP32.
    Si el ESP32 tiene el firmware nuevo, responde END,<n> enseguida y mantiene el socket.
    Si el ESP32 tiene el firmware original (que no lee STOP), retorna False para reiniciar
    limpiamente el socket y que el ESP32 pare de inmediato y se reconecte solo.
    """
    cerrar_sesion_activa(estado_sesion, mensaje_motivo, cortado_por_stop=True)

    try:
        conexion.sendall(b"STOP\n")
    except OSError:
        return False

    t_espera = time.perf_counter()
    buffer_temp = b""

    while (time.perf_counter() - t_espera) < 0.22:
        listos, _, _ = select.select([conexion, servidor_udp], [], [], 0.02)

        if servidor_udp in listos:
            while True:
                try:
                    servidor_udp.recvfrom(4096)
                except BlockingIOError:
                    break

        if conexion in listos:
            try:
                bloque = conexion.recv(4096)
            except BlockingIOError:
                continue
            if not bloque:
                return False
            buffer_temp += bloque
            if b"END," in buffer_temp or b"ERR," in buffer_temp:
                # El ESP32 detuvo el bucle limpiamente con el nuevo firmware
                return True

    # El ESP32 sigue enviando datos (firmware sin soporte de STOP): reiniciar conexion TCP
    return False


def armar_comando_start(frecuencia, rango, cantidad, modo, protocolo):
    """
    Si el protocolo es TCP, envia los 4 parametros clasicos (100% compatible con
    firmware antiguo y nuevo). Si es UDP, agrega el 5to parametro UDP.
    """
    if protocolo == "UDP":
        return f"START {frecuencia} {rango} {cantidad} {modo} UDP\n"
    return f"START {frecuencia} {rango} {cantidad} {modo}\n"


def servidor_tcp():

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor, \
             socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as servidor_udp:

            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor_udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            servidor.bind((HOST, PUERTO))
            servidor.listen(1)

            servidor_udp.bind((HOST, PUERTO))
            servidor_udp.setblocking(False)

            emitir("estado", "Esperando conexion ESP32...")

            while True:

                conexion, direccion = servidor.accept()
                conexion.setblocking(False)

                emitir("conexion", direccion[0])

                buffer = b""
                reinicio_por_stop = False
                estado_sesion = {
                    "archivo": None,
                    "escritor": None,
                    "ruta": None,
                    "activa": False,
                    "config": None,
                    "guardadas": 0,
                    "enviadas_acumuladas": 0,
                    "seq_offset": 0,
                    "t_ms_inicial": None,
                    "t_inicio_pc": None,
                    "t_ultimo_pc": None,
                    "t_comando_pc": None,
                    "ultimo_arribo_pc": None,
                    "intervalos_ms": [],
                    "ultimo_seq": None,
                    "perdidos_seq": 0,
                    "fuera_de_orden": 0,
                    "bytes_recibidos": 0,
                    "stop_enviado": False,
                    "tiempo_cumplido": False
                }

                try:
                    with conexion:

                        while True:

                            # Comandos enviados desde GUI
                            try:
                                orden = ordenes.get_nowait()
                            except queue.Empty:
                                orden = None

                            if orden is not None:
                                accion = orden[0]

                                if accion == "STOP" and estado_sesion["activa"]:
                                    sigue_conectado = detener_esp32_y_cerrar(
                                        conexion,
                                        servidor_udp,
                                        estado_sesion,
                                        f"END,{estado_sesion['guardadas']} (Detenido)"
                                    )
                                    buffer = b""
                                    if not sigue_conectado:
                                        reinicio_por_stop = True
                                        break

                                elif accion == "START" and not estado_sesion["activa"]:
                                    _, frecuencia, rango, cantidad, modo, protocolo, duracion_s, es_continuo = orden

                                    # Limpiar datagramas UDP antiguos antes de iniciar
                                    while True:
                                        try:
                                            servidor_udp.recvfrom(4096)
                                        except BlockingIOError:
                                            break

                                    comando = armar_comando_start(
                                        frecuencia, rango, cantidad, modo, protocolo
                                    )

                                    conexion.sendall(comando.encode("utf-8"))

                                    fecha = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                                    ruta = CARPETA / f"bmi270_{protocolo.lower()}_{fecha}.csv"

                                    archivo = open(ruta, "w", newline="", encoding="utf-8")
                                    escritor = csv.DictWriter(archivo, fieldnames=COLUMNAS)
                                    escritor.writeheader()

                                    estado_sesion.update({
                                        "archivo": archivo,
                                        "escritor": escritor,
                                        "ruta": ruta,
                                        "config": (frecuencia, rango, cantidad, modo, protocolo, duracion_s, es_continuo),
                                        "guardadas": 0,
                                        "enviadas_acumuladas": 0,
                                        "seq_offset": 0,
                                        "t_ms_inicial": None,
                                        "activa": True,
                                        "t_inicio_pc": None,
                                        "t_ultimo_pc": None,
                                        "t_comando_pc": time.perf_counter(),
                                        "ultimo_arribo_pc": None,
                                        "intervalos_ms": [],
                                        "ultimo_seq": None,
                                        "perdidos_seq": 0,
                                        "fuera_de_orden": 0,
                                        "bytes_recibidos": 0,
                                        "stop_enviado": False,
                                        "tiempo_cumplido": False
                                    })

                                    emitir("archivo", str(ruta))
                                    if es_continuo:
                                        emitir("iniciada", f"MODO CONTINUO ({frecuencia} Hz, ±{rango}g, {modo}, {protocolo})")
                                    elif duracion_s is not None:
                                        emitir("iniciada", f"MODO TIEMPO: {duracion_s:g} s ({frecuencia} Hz, ±{rango}g, {modo}, {protocolo})")
                                    else:
                                        emitir("iniciada", comando.strip())

                            # Verificar si en modo Por Tiempo (s) ya se cumplio la duracion exacta
                            if estado_sesion["activa"] and estado_sesion["config"] is not None:
                                _, _, _, _, _, duracion_s, _ = estado_sesion["config"]
                                if duracion_s is not None:
                                    tiempo_pc_cumplido = (
                                        estado_sesion["t_inicio_pc"] is not None and
                                        (time.perf_counter() - estado_sesion["t_inicio_pc"]) >= duracion_s
                                    )
                                    if estado_sesion["tiempo_cumplido"] or tiempo_pc_cumplido:
                                        estado_sesion["tiempo_cumplido"] = True
                                        sigue_conectado = detener_esp32_y_cerrar(
                                            conexion,
                                            servidor_udp,
                                            estado_sesion,
                                            f"END,{estado_sesion['guardadas']} ({duracion_s:g} s completados)"
                                        )
                                        buffer = b""
                                        if not sigue_conectado:
                                            reinicio_por_stop = True
                                            break
                                        continue

                            # Esperar eventos de red en TCP o UDP sin bloqueo artificial
                            listos, _, _ = select.select([conexion, servidor_udp], [], [], 0.02)

                            # Recibir datagramas UDP si llegaron
                            if servidor_udp in listos:
                                while True:
                                    try:
                                        datagrama, _ = servidor_udp.recvfrom(4096)
                                        texto_udp = datagrama.decode("utf-8", errors="ignore").strip()
                                        if texto_udp:
                                            procesar_linea_imu(texto_udp, estado_sesion)
                                    except BlockingIOError:
                                        break

                            if conexion not in listos:
                                continue

                            # Recibir bytes TCP
                            try:
                                bloque = conexion.recv(4096)
                            except BlockingIOError:
                                continue

                            if not bloque:
                                break

                            buffer += bloque

                            if len(buffer) > 65536:
                                emitir("error", "Mensaje demasiado largo")
                                buffer = b""
                                continue

                            # Una linea = un mensaje completo
                            while b"\n" in buffer:

                                linea, buffer = buffer.split(b"\n", 1)

                                try:
                                    texto = linea.decode("utf-8").strip()
                                except UnicodeDecodeError:
                                    emitir("error", "Texto no valido")
                                    continue

                                if not texto:
                                    continue

                                # Vaciar posibles paquetes UDP remanentes antes de evaluar fin de lote
                                if texto.startswith("END,") or texto.startswith("ERR,"):
                                    time.sleep(0.02)
                                    while True:
                                        try:
                                            datagrama, _ = servidor_udp.recvfrom(4096)
                                            texto_udp = datagrama.decode("utf-8", errors="ignore").strip()
                                            if texto_udp:
                                                procesar_linea_imu(texto_udp, estado_sesion)
                                        except BlockingIOError:
                                            break

                                # ESP32 confirma final de lote o sesion
                                if texto.startswith("END,"):
                                    if not estado_sesion["activa"]:
                                        continue

                                    f, r, cantidad_cfg, m, proto, duracion_s, es_continuo = estado_sesion["config"]

                                    # Si estamos en modo Continuo (Inf) o Por Tiempo (s) y aun no termina el tiempo,
                                    # encadenar automaticamente otro lote de 10000 muestras sin detener la grafica
                                    if (es_continuo or duracion_s is not None) and not estado_sesion["tiempo_cumplido"]:
                                        partes = texto.split(",")
                                        if len(partes) >= 2 and partes[1].strip().isdigit():
                                            estado_sesion["enviadas_acumuladas"] += int(partes[1].strip())
                                        if estado_sesion["ultimo_seq"] is not None:
                                            estado_sesion["seq_offset"] = estado_sesion["ultimo_seq"] + 1

                                        cmd_renovacion = armar_comando_start(f, r, cantidad_cfg, m, proto)
                                        conexion.sendall(cmd_renovacion.encode("utf-8"))
                                        continue

                                    cerrar_sesion_activa(estado_sesion, texto, cortado_por_stop=False)
                                    continue

                                # ESP32 informa error
                                if texto.startswith("ERR,"):
                                    emitir("error", texto)

                                    if estado_sesion["archivo"] is not None:
                                        estado_sesion["archivo"].close()
                                        estado_sesion["archivo"] = None
                                        estado_sesion["escritor"] = None

                                    estado_sesion["activa"] = False
                                    emitir("fin", {
                                        "mensaje": texto,
                                        "guardadas": estado_sesion["guardadas"],
                                        "resumen": None
                                    })
                                    continue

                                # Interpretar paquete JSON por TCP
                                procesar_linea_imu(texto, estado_sesion)

                except (OSError, UnicodeError) as error:
                    emitir("error", str(error))

                finally:

                    if estado_sesion["archivo"] is not None:
                        estado_sesion["archivo"].close()

                    if reinicio_por_stop:
                        emitir("reconectando", None)
                    else:
                        emitir("desconexion", None)

    except OSError as error:
        emitir("error", f"Servidor TCP/UDP: {error}")


# ========================================
# VENTANA GRAFICA
# ========================================

COLOR_BG = "#0F172A"          # Azul pizarra oscuro principal
COLOR_CARD = "#1E293B"        # Superficie de tarjetas
COLOR_BORDER = "#334155"      # Bordes sutiles
COLOR_TEXT = "#F8FAFC"        # Texto principal blanco humo
COLOR_MUTED = "#94A3B8"       # Texto secundario gris azulado
COLOR_ACCENT = "#38BDF8"      # Cian brillante
COLOR_SUCCESS = "#10B981"     # Verde esmeralda
COLOR_WARNING = "#F59E0B"     # Ambar
COLOR_DANGER = "#EF4444"      # Rojo coral

ventana = tk.Tk()
ventana.title("TIC3 - Sistema de Telemetría IMU BMI270 (TCP / UDP)")
ventana.geometry("1240x880")
ventana.configure(bg=COLOR_BG)

# Configuracion de estilo moderno (ttk)
estilo = ttk.Style(ventana)
estilo.theme_use("clam")

estilo.configure(".", background=COLOR_BG, foreground=COLOR_TEXT, font=("Segoe UI", 10))
estilo.configure("TFrame", background=COLOR_BG)
estilo.configure("Card.TFrame", background=COLOR_CARD, relief="flat")
estilo.configure("TLabel", background=COLOR_BG, foreground=COLOR_TEXT)
estilo.configure("Card.TLabel", background=COLOR_CARD, foreground=COLOR_TEXT)
estilo.configure("Muted.TLabel", background=COLOR_CARD, foreground=COLOR_MUTED, font=("Segoe UI", 9))
estilo.configure("Header.TLabel", background=COLOR_BG, foreground=COLOR_TEXT, font=("Segoe UI", 18, "bold"))
estilo.configure("SubHeader.TLabel", background=COLOR_BG, foreground=COLOR_ACCENT, font=("Segoe UI", 11, "bold"))

estilo.configure(
    "TLabelframe",
    background=COLOR_CARD,
    foreground=COLOR_ACCENT,
    bordercolor=COLOR_BORDER,
    relief="solid",
    borderwidth=1
)
estilo.configure(
    "TLabelframe.Label",
    background=COLOR_CARD,
    foreground=COLOR_ACCENT,
    font=("Segoe UI", 10, "bold")
)

estilo.configure(
    "TCombobox",
    fieldbackground=COLOR_BG,
    background=COLOR_BORDER,
    foreground=COLOR_TEXT,
    arrowcolor=COLOR_ACCENT
)
estilo.map(
    "TCombobox",
    fieldbackground=[("readonly", COLOR_BG), ("!disabled", COLOR_BG)],
    foreground=[("readonly", COLOR_TEXT), ("!disabled", COLOR_TEXT)],
    selectbackground=[("readonly", COLOR_BG), ("!disabled", COLOR_BG)],
    selectforeground=[("readonly", COLOR_ACCENT), ("!disabled", COLOR_ACCENT)]
)

estilo.configure(
    "TEntry",
    fieldbackground=COLOR_BG,
    foreground=COLOR_TEXT,
    insertcolor=COLOR_TEXT
)

estilo.configure(
    "Start.TButton",
    background=COLOR_SUCCESS,
    foreground="#FFFFFF",
    font=("Segoe UI", 10, "bold"),
    padding=(14, 6),
    borderwidth=0
)
estilo.map(
    "Start.TButton",
    background=[("active", "#059669"), ("disabled", COLOR_BORDER)],
    foreground=[("disabled", COLOR_MUTED)]
)

estilo.configure(
    "Stop.TButton",
    background=COLOR_DANGER,
    foreground="#FFFFFF",
    font=("Segoe UI", 10, "bold"),
    padding=(14, 6),
    borderwidth=0
)
estilo.map(
    "Stop.TButton",
    background=[("active", "#DC2626"), ("disabled", COLOR_BORDER)],
    foreground=[("disabled", COLOR_MUTED)]
)

estilo.configure(
    "TRadiobutton",
    background=COLOR_CARD,
    foreground=COLOR_TEXT,
    font=("Segoe UI", 10, "bold"),
    indicatorcolor=COLOR_ACCENT
)
estilo.map(
    "TRadiobutton",
    background=[("active", COLOR_CARD)],
    foreground=[("selected", COLOR_ACCENT)]
)

estado = tk.StringVar(value="Iniciando servidor...")
contador = tk.StringVar(value="Muestras: 0  |  Tiempo: 0.00 s")
archivo_actual = tk.StringVar(value="Sin sesion iniciada")
aviso = tk.StringVar(value="Servidor listo para configuracion")

conectado = False
sesion_activa = False
total = 0
tiempo_inicial = None

# Historial completo para permitir grafica continua hasta el infinito, ventana por tiempo o ultimas 300 muestras
historial_completo = []
historial = deque(maxlen=300)

# Metricas almacenadas para comparar TCP vs UDP
metricas_comparacion = {
    "TCP": None,
    "UDP": None
}

# ========================================
# CABECERA SUPERIOR
# ========================================

barra_superior = ttk.Frame(ventana, padding=(18, 10))
barra_superior.pack(fill="x")

ttk.Label(
    barra_superior,
    text="SISTEMA DE ADQUISICIÓN Y TELEMETRÍA BMI270",
    style="Header.TLabel"
).pack(side="left")

badge_estado = tk.Label(
    barra_superior,
    textvariable=estado,
    bg="#334155",
    fg=COLOR_TEXT,
    font=("Segoe UI", 10, "bold"),
    padx=14,
    pady=5
)
badge_estado.pack(side="right")

# ========================================
# PANEL DE CONFIGURACION
# ========================================

panel_config = ttk.LabelFrame(
    ventana, text=" Configuración de Adquisición y Transporte ",
    padding=10
)
panel_config.pack(fill="x", padx=18, pady=5)

frecuencia_var = tk.StringVar(value="50")
rango_var = tk.StringVar(value="4")
tipo_duracion_var = tk.StringVar(value="Por Muestras")
cantidad_var = tk.StringVar(value="100")
modo_var = tk.StringVar(value="SECUENCIAL")
protocolo_var = tk.StringVar(value="TCP")

ultimo_muestras_str = "100"
ultimo_tiempo_str = "10"

ttk.Label(panel_config, text="Frecuencia (Hz)", style="Muted.TLabel").grid(row=0, column=0, padx=6, pady=2)
ttk.Combobox(
    panel_config,
    textvariable=frecuencia_var,
    values=["25", "50", "100"],
    state="readonly",
    width=9
).grid(row=1, column=0, padx=6, pady=4)

ttk.Label(panel_config, text="Rango (+/- g)", style="Muted.TLabel").grid(row=0, column=1, padx=6, pady=2)
ttk.Combobox(
    panel_config,
    textvariable=rango_var,
    values=["2", "4", "8", "16"],
    state="readonly",
    width=9
).grid(row=1, column=1, padx=6, pady=4)

ttk.Label(panel_config, text="Modalidad Duración", style="Muted.TLabel").grid(row=0, column=2, padx=6, pady=2)
combo_duracion = ttk.Combobox(
    panel_config,
    textvariable=tipo_duracion_var,
    values=["Por Muestras", "Por Tiempo (s)", "Continuo (Inf)"],
    state="readonly",
    width=15
)
combo_duracion.grid(row=1, column=2, padx=6, pady=4)

etiqueta_valor_duracion = ttk.Label(panel_config, text="Cantidad de muestras", style="Muted.TLabel")
etiqueta_valor_duracion.grid(row=0, column=3, padx=6, pady=2)

entrada_cantidad = ttk.Entry(
    panel_config,
    textvariable=cantidad_var,
    width=12
)
entrada_cantidad.grid(row=1, column=3, padx=6, pady=4)


def al_cambiar_modalidad_duracion(*_):
    global ultimo_muestras_str, ultimo_tiempo_str
    modalidad = tipo_duracion_var.get()
    valor_actual = cantidad_var.get().strip()

    if modalidad == "Por Muestras":
        if valor_actual not in ("∞", "") and valor_actual.replace(".", "", 1).isdigit():
            if float(valor_actual) < 50:
                ultimo_tiempo_str = valor_actual
        etiqueta_valor_duracion.config(text="Cantidad de muestras")
        entrada_cantidad.config(state="normal")
        cantidad_var.set(ultimo_muestras_str)

    elif modalidad == "Por Tiempo (s)":
        if valor_actual not in ("∞", "") and valor_actual.isdigit():
            if int(valor_actual) >= 50:
                ultimo_muestras_str = valor_actual
        etiqueta_valor_duracion.config(text="Duración (segundos)")
        entrada_cantidad.config(state="normal")
        cantidad_var.set(ultimo_tiempo_str)

    else:
        if valor_actual not in ("∞", ""):
            if valor_actual.isdigit() and int(valor_actual) >= 50:
                ultimo_muestras_str = valor_actual
            elif valor_actual.replace(".", "", 1).isdigit():
                ultimo_tiempo_str = valor_actual
        etiqueta_valor_duracion.config(text="Límite (Infinito)")
        cantidad_var.set("∞")
        entrada_cantidad.config(state="disabled")


tipo_duracion_var.trace_add("write", al_cambiar_modalidad_duracion)

ttk.Label(panel_config, text="Modo Lectura", style="Muted.TLabel").grid(row=0, column=4, padx=6, pady=2)
ttk.Combobox(
    panel_config,
    textvariable=modo_var,
    values=["SECUENCIAL", "PARALELO"],
    state="readonly",
    width=13
).grid(row=1, column=4, padx=6, pady=4)

ttk.Label(panel_config, text="Protocolo Red", style="Muted.TLabel").grid(row=0, column=5, padx=6, pady=2)
ttk.Combobox(
    panel_config,
    textvariable=protocolo_var,
    values=["TCP", "UDP"],
    state="readonly",
    width=9
).grid(row=1, column=5, padx=6, pady=4)


def iniciar_adquisicion():

    global sesion_activa, ultimo_muestras_str, ultimo_tiempo_str

    if not conectado or sesion_activa:
        return

    try:
        frecuencia = int(frecuencia_var.get())
        rango = int(rango_var.get())
        modalidad = tipo_duracion_var.get()
        duracion_s = None
        es_continuo = False

        if modalidad == "Por Muestras":
            cantidad = int(cantidad_var.get())
            if cantidad < 1 or cantidad > 10000:
                raise ValueError("Cantidad fuera de rango")
            ultimo_muestras_str = str(cantidad)

        elif modalidad == "Por Tiempo (s)":
            segundos = float(cantidad_var.get())
            if segundos <= 0 or segundos > 3600:
                raise ValueError("Tiempo fuera de rango")
            duracion_s = segundos
            ultimo_tiempo_str = cantidad_var.get().strip()
            # Pedimos un lote amplio (10000) compatible con cualquier firmware ESP32
            # y el temporizador de la Raspberry corta exactamente al cumplirse duracion_s
            cantidad = LOTE_CONTINUO

        else:
            # Modo Continuo hasta el infinito: pide lotes de 10000 que se renuevan solos
            # hasta que el usuario presione el boton DETENER
            cantidad = LOTE_CONTINUO
            es_continuo = True

    except ValueError:
        aviso.set("Error: ingrese muestras entre 1 y 10000, o segundos mayores a 0")
        return

    modo = modo_var.get()
    protocolo = protocolo_var.get()

    sesion_activa = True
    boton_iniciar.config(state="disabled")
    boton_detener.config(state="normal")

    ordenes.put((
        "START", frecuencia, rango, cantidad, modo, protocolo, duracion_s, es_continuo
    ))

    estado.set(f"Enviando config ({protocolo})...")
    aviso.set(f"Esperando muestras del ESP32 vía {protocolo}...")


def detener_adquisicion():
    if sesion_activa:
        ordenes.put(("STOP",))
        boton_detener.config(state="disabled")
        aviso.set("Deteniendo adquisición...")


boton_iniciar = ttk.Button(
    panel_config,
    text="▶ INICIAR",
    style="Start.TButton",
    command=iniciar_adquisicion,
    state="disabled"
)
boton_iniciar.grid(row=0, column=6, rowspan=2, padx=6, pady=4)

boton_detener = ttk.Button(
    panel_config,
    text="■ DETENER",
    style="Stop.TButton",
    command=detener_adquisicion,
    state="disabled"
)
boton_detener.grid(row=0, column=7, rowspan=2, padx=6, pady=4)

for columna in range(8):
    panel_config.columnconfigure(columna, weight=1)

# ========================================
# VALORES DEL SENSOR Y COMPARATIVA TCP vs UDP
# ========================================

fila_media = ttk.Frame(ventana)
fila_media.pack(fill="x", padx=18, pady=5)

panel_valores = ttk.LabelFrame(
    fila_media,
    text=" Mediciones en Tiempo Real ",
    padding=8
)
panel_valores.pack(side="left", fill="both", expand=True, padx=(0, 6))

nombres = [
    ("acc_x_ms2", "Aceleración X (m/s²)", "#38BDF8"),
    ("acc_y_ms2", "Aceleración Y (m/s²)", "#F59E0B"),
    ("acc_z_ms2", "Aceleración Z (m/s²)", "#10B981"),
    ("gyr_x_rads", "Giroscopio X (rad/s)", "#38BDF8"),
    ("gyr_y_rads", "Giroscopio Y (rad/s)", "#F59E0B"),
    ("gyr_z_rads", "Giroscopio Z (rad/s)", "#10B981")
]

valores = {}

for i, (clave, nombre, color_eje) in enumerate(nombres):

    celda = tk.Frame(
        panel_valores,
        bg="#0F172A",
        highlightbackground=COLOR_BORDER,
        highlightthickness=1,
        padx=10,
        pady=6
    )
    celda.grid(
        row=i // 3,
        column=i % 3,
        sticky="nsew",
        padx=4,
        pady=4
    )

    tk.Label(
        celda,
        text=nombre,
        bg="#0F172A",
        fg=COLOR_MUTED,
        font=("Segoe UI", 9)
    ).pack()

    variable = tk.StringVar(value="--")
    valores[clave] = variable

    tk.Label(
        celda,
        textvariable=variable,
        bg="#0F172A",
        fg=color_eje,
        font=("Consolas", 15, "bold")
    ).pack()

for i in range(3):
    panel_valores.columnconfigure(i, weight=1)

# Panel de Comparacion TCP vs UDP
panel_comparacion = ttk.LabelFrame(
    fila_media,
    text=" Comparativa de Transporte: TCP vs UDP ",
    padding=8
)
panel_comparacion.pack(side="right", fill="both", expand=True, padx=(6, 0))

encabezados_comp = ["Métrica", "Última Sesión TCP", "Última Sesión UDP"]
for col_idx, texto_enc in enumerate(encabezados_comp):
    ttk.Label(
        panel_comparacion,
        text=texto_enc,
        font=("Segoe UI", 9, "bold"),
        foreground=COLOR_ACCENT if col_idx > 0 else COLOR_MUTED,
        style="Card.TLabel"
    ).grid(row=0, column=col_idx, padx=8, pady=2, sticky="w" if col_idx == 0 else "")

filas_comp_definicion = [
    ("recibidas", "Recibidas / Enviadas"),
    ("perdida", "Pérdida de Paquetes"),
    ("frecuencia", "Frecuencia Efectiva"),
    ("jitter", "Intervalo Δt / Jitter"),
    ("throughput", "Throughput Red")
]

etiquetas_comp = {"TCP": {}, "UDP": {}}

for row_idx, (clave_m, etiqueta_m) in enumerate(filas_comp_definicion, start=1):
    ttk.Label(
        panel_comparacion,
        text=etiqueta_m,
        style="Muted.TLabel"
    ).grid(row=row_idx, column=0, padx=8, pady=2, sticky="w")

    var_tcp = tk.StringVar(value="Sin datos")
    var_udp = tk.StringVar(value="Sin datos")
    etiquetas_comp["TCP"][clave_m] = var_tcp
    etiquetas_comp["UDP"][clave_m] = var_udp

    ttk.Label(
        panel_comparacion,
        textvariable=var_tcp,
        font=("Consolas", 9, "bold"),
        style="Card.TLabel"
    ).grid(row=row_idx, column=1, padx=8, pady=2)

    ttk.Label(
        panel_comparacion,
        textvariable=var_udp,
        font=("Consolas", 9, "bold"),
        style="Card.TLabel"
    ).grid(row=row_idx, column=2, padx=8, pady=2)

veredicto_comp_var = tk.StringVar(
    value="Ejecute una sesión en TCP y otra en UDP para comparar pérdida y latencia."
)
ttk.Label(
    panel_comparacion,
    textvariable=veredicto_comp_var,
    foreground=COLOR_SUCCESS,
    font=("Segoe UI", 9, "italic"),
    style="Card.TLabel"
).grid(row=len(filas_comp_definicion) + 1, column=0, columnspan=3, padx=8, pady=(4, 0), sticky="w")

for col_idx in range(3):
    panel_comparacion.columnconfigure(col_idx, weight=1)

# ========================================
# CONTROLES DE VISUALIZACION Y GRAFICO
# ========================================

panel_controles_grafica = ttk.LabelFrame(
    ventana,
    text=" Controles de Gráfica en Tiempo Real ",
    padding=8
)
panel_controles_grafica.pack(fill="x", padx=18, pady=4)

sensor_grafica_var = tk.StringVar(value="ACELERACION")
vista_grafica_var = tk.StringVar(value="Continua (Inf)")
ventana_tiempo_var = tk.StringVar(value="10")

ttk.Label(
    panel_controles_grafica,
    text="Sensor a visualizar:",
    style="Muted.TLabel"
).pack(side="left", padx=(6, 4))

ttk.Radiobutton(
    panel_controles_grafica,
    text="Aceleración (m/s²)",
    variable=sensor_grafica_var,
    value="ACELERACION",
    command=lambda: redibujar_grafica(forzar_ejes=True)
).pack(side="left", padx=6)

ttk.Radiobutton(
    panel_controles_grafica,
    text="Giroscopio (rad/s)",
    variable=sensor_grafica_var,
    value="GIROSCOPIO",
    command=lambda: redibujar_grafica(forzar_ejes=True)
).pack(side="left", padx=6)

ttk.Label(
    panel_controles_grafica,
    text="   |   Modo de Gráfica (Tiempo):",
    style="Muted.TLabel"
).pack(side="left", padx=(14, 4))

combo_vista_grafica = ttk.Combobox(
    panel_controles_grafica,
    textvariable=vista_grafica_var,
    values=["Continua (Inf)", "Ventana de Tiempo (s)", "Ultimas 300 muestras"],
    state="readonly",
    width=22
)
combo_vista_grafica.pack(side="left", padx=6)

ttk.Label(
    panel_controles_grafica,
    text="Ventana (s):",
    style="Muted.TLabel"
).pack(side="left", padx=(10, 4))

combo_ventana_s = ttk.Combobox(
    panel_controles_grafica,
    textvariable=ventana_tiempo_var,
    values=["2", "5", "10", "20", "30", "60"],
    state="normal",
    width=6
)
combo_ventana_s.pack(side="left", padx=4)

ttk.Label(
    panel_controles_grafica,
    textvariable=contador,
    font=("Consolas", 10, "bold"),
    foreground=COLOR_ACCENT,
    style="Card.TLabel"
).pack(side="right", padx=10)

# Figura Matplotlib con estetica moderna oscura
figura = Figure(figsize=(9.5, 3.8), dpi=100, facecolor=COLOR_CARD)
grafica = figura.add_subplot(111)
grafica.set_facecolor("#0F172A")

for spine in grafica.spines.values():
    spine.set_color(COLOR_BORDER)

grafica.tick_params(colors=COLOR_MUTED, labelsize=9)
grafica.set_title("Aceleración en tiempo real", color=COLOR_TEXT, fontsize=12, fontweight="bold", pad=10)
grafica.set_xlabel("Tiempo relativo (s)", color=COLOR_MUTED, fontsize=10)
grafica.set_ylabel("Aceleración (m/s²)", color=COLOR_MUTED, fontsize=10)
grafica.grid(True, linestyle="--", alpha=0.35, color=COLOR_BORDER)

linea_x, = grafica.plot([], [], label="Eje X", color="#38BDF8", linewidth=1.6)
linea_y, = grafica.plot([], [], label="Eje Y", color="#F59E0B", linewidth=1.6)
linea_z, = grafica.plot([], [], label="Eje Z", color="#10B981", linewidth=1.6)

leyenda = grafica.legend(loc="upper right", facecolor=COLOR_CARD, edgecolor=COLOR_BORDER)
for text in leyenda.get_texts():
    text.set_color(COLOR_TEXT)

figura.tight_layout()

canvas = FigureCanvasTkAgg(
    figura, master=ventana
)
canvas.get_tk_widget().configure(bg=COLOR_CARD, highlightbackground=COLOR_BORDER, highlightthickness=1)
canvas.get_tk_widget().pack(
    fill="both", expand=True,
    padx=18, pady=6
)

# Barra inferior de estado y archivo
barra_inferior = ttk.Frame(ventana, padding=(18, 4))
barra_inferior.pack(fill="x")

ttk.Label(
    barra_inferior, textvariable=archivo_actual,
    foreground=COLOR_MUTED,
    wraplength=600
).pack(side="left")

ttk.Label(
    barra_inferior, textvariable=aviso,
    foreground=COLOR_ACCENT,
    font=("Segoe UI", 9, "bold"),
    wraplength=580
).pack(side="right")


# ========================================
# FUNCIONES DE GRAFICADO Y COMPARACION
# ========================================

def actualizar_tabla_comparativa(resumen):
    if resumen is None:
        return

    proto = resumen["protocolo"]
    metricas_comparacion[proto] = resumen

    etiquetas_comp[proto]["recibidas"].set(
        f"{resumen['recibidas']} / {resumen['enviadas']} ({resumen['modo'][:3]})"
    )
    etiquetas_comp[proto]["perdida"].set(
        f"{resumen['perdidos']} paq ({resumen['perdida_pct']:.1f}%)"
    )
    etiquetas_comp[proto]["frecuencia"].set(
        f"{resumen['frec_efectiva_hz']:.1f} Hz (cfg {resumen['frecuencia_cfg']} Hz)"
    )
    etiquetas_comp[proto]["jitter"].set(
        f"{resumen['dt_prom_ms']:.1f} ms (±{resumen['jitter_ms']:.1f} ms)"
    )
    etiquetas_comp[proto]["throughput"].set(
        f"{resumen['throughput_kbs']:.2f} KB/s ({resumen['duracion_s']:.1f} s)"
    )

    m_tcp = metricas_comparacion["TCP"]
    m_udp = metricas_comparacion["UDP"]
    if m_tcp and m_udp:
        veredicto_comp_var.set(
            f"Comparación: TCP tuvo {m_tcp['perdida_pct']:.1f}% pérdida vs {m_udp['perdida_pct']:.1f}% en UDP | "
            f"Jitter TCP: ±{m_tcp['jitter_ms']:.1f} ms vs UDP: ±{m_udp['jitter_ms']:.1f} ms."
        )


def redibujar_grafica(forzar_ejes=False):
    sensor_sel = sensor_grafica_var.get()
    modo_vista = vista_grafica_var.get()

    if forzar_ejes:
        if sensor_sel == "ACELERACION":
            grafica.set_title("Aceleración en tiempo real (BMI270)", color=COLOR_TEXT, fontsize=12, fontweight="bold")
            grafica.set_ylabel("Aceleración (m/s²)", color=COLOR_MUTED, fontsize=10)
        else:
            grafica.set_title("Giroscopio en tiempo real (BMI270)", color=COLOR_TEXT, fontsize=12, fontweight="bold")
            grafica.set_ylabel("Velocidad angular (rad/s)", color=COLOR_MUTED, fontsize=10)

    if not historial_completo:
        linea_x.set_data([], [])
        linea_y.set_data([], [])
        linea_z.set_data([], [])
        grafica.set_xlim(0.0, 10.0)
        canvas.draw_idle()
        return

    t_max_total = historial_completo[-1][0]

    # Seleccionar subconjunto de datos y limites del eje X segun el modo de grafica elegido
    if modo_vista == "Ultimas 300 muestras":
        datos_vista = list(historial)
        x_min = datos_vista[0][0]
        x_max = max(x_min + 0.5, datos_vista[-1][0])
    elif modo_vista == "Ventana de Tiempo (s)":
        try:
            ventana_s = float(ventana_tiempo_var.get())
            if ventana_s <= 0:
                ventana_s = 10.0
        except ValueError:
            ventana_s = 10.0
        x_min = max(0.0, t_max_total - ventana_s)
        x_max = max(x_min + ventana_s, t_max_total)
        datos_vista = [p for p in historial_completo if p[0] >= x_min]
    else:
        # Continua (Inf): muestra toda la serie temporal desde 0 s hasta el presente
        n_total = len(historial_completo)
        paso = max(1, n_total // 2500)
        datos_vista = historial_completo[::paso]
        if datos_vista[-1] != historial_completo[-1]:
            datos_vista.append(historial_completo[-1])
        x_min = 0.0
        x_max = max(1.0, t_max_total * 1.02)

    tiempos = [p[0] for p in datos_vista]

    if sensor_sel == "ACELERACION":
        xs = [p[1] for p in datos_vista]
        ys = [p[2] for p in datos_vista]
        zs = [p[3] for p in datos_vista]
    else:
        xs = [p[4] for p in datos_vista]
        ys = [p[5] for p in datos_vista]
        zs = [p[6] for p in datos_vista]

    linea_x.set_data(tiempos, xs)
    linea_y.set_data(tiempos, ys)
    linea_z.set_data(tiempos, zs)

    grafica.set_xlim(x_min, x_max)
    grafica.relim()
    grafica.autoscale_view(scalex=False, scaley=True)
    canvas.draw_idle()


def al_cambiar_ventana_s(*_):
    if vista_grafica_var.get() != "Ventana de Tiempo (s)":
        vista_grafica_var.set("Ventana de Tiempo (s)")
    redibujar_grafica(forzar_ejes=False)


combo_vista_grafica.bind("<<ComboboxSelected>>", lambda _: redibujar_grafica(forzar_ejes=False))
combo_ventana_s.bind("<<ComboboxSelected>>", al_cambiar_ventana_s)
combo_ventana_s.bind("<Return>", al_cambiar_ventana_s)


# ========================================
# ACTUALIZACION DE LA GUI
# ========================================

def actualizar():

    global conectado, sesion_activa
    global total, tiempo_inicial

    cambiar_grafico = False

    for _ in range(500):

        try:
            tipo, dato = eventos.get_nowait()
        except queue.Empty:
            break

        if tipo == "estado":
            estado.set(dato)

        elif tipo == "conexion":
            conectado = True
            estado.set(f"● ESP32 conectado ({dato})")
            badge_estado.config(bg=COLOR_SUCCESS)

            if not sesion_activa:
                boton_iniciar.config(state="normal")
                aviso.set("ESP32 conectado y listo para iniciar adquisición")

        elif tipo == "reconectando":
            conectado = False
            sesion_activa = False
            estado.set("● Reiniciando enlace ESP32...")
            badge_estado.config(bg=COLOR_WARNING)
            boton_iniciar.config(state="disabled")
            boton_detener.config(state="disabled")

        elif tipo == "desconexion":
            conectado = False
            sesion_activa = False
            estado.set("○ ESP32 desconectado")
            badge_estado.config(bg=COLOR_DANGER)
            boton_iniciar.config(state="disabled")
            boton_detener.config(state="disabled")

        elif tipo == "iniciada":

            total = 0
            tiempo_inicial = None
            historial.clear()
            historial_completo.clear()

            contador.set("Muestras: 0  |  Tiempo: 0.00 s")
            estado.set(f"● Adquisición activa ({protocolo_var.get()})")
            badge_estado.config(bg="#0284C7")
            aviso.set(str(dato))
            redibujar_grafica(forzar_ejes=True)

        elif tipo == "archivo":
            archivo_actual.set("CSV: " + dato)

        elif tipo == "aviso":
            aviso.set(str(dato))

        elif tipo == "dato":

            total += 1

            for clave, _, _ in nombres:
                valores[clave].set(
                    f"{dato[clave]:.3f}"
                )

            if "t_rel" in dato:
                t_rel = float(dato["t_rel"])
            else:
                tiempo = dato["t_ms"] / 1000.0
                if tiempo_inicial is None:
                    tiempo_inicial = tiempo
                t_rel = max(0.0, tiempo - tiempo_inicial)

            contador.set(f"Muestras: {total}  |  Tiempo: {t_rel:.2f} s")

            punto = (
                t_rel,
                dato["acc_x_ms2"],
                dato["acc_y_ms2"],
                dato["acc_z_ms2"],
                dato["gyr_x_rads"],
                dato["gyr_y_rads"],
                dato["gyr_z_rads"]
            )

            historial.append(punto)
            historial_completo.append(punto)

            cambiar_grafico = True

        elif tipo == "fin":

            sesion_activa = False
            estado.set("● Adquisición terminada")
            badge_estado.config(bg=COLOR_SUCCESS if conectado else COLOR_WARNING)
            boton_detener.config(state="disabled")

            if dato.get("resumen"):
                actualizar_tabla_comparativa(dato["resumen"])

            aviso.set(
                f"{dato['mensaje']} | "
                f"Guardadas: {dato['guardadas']}"
            )

            if conectado:
                boton_iniciar.config(state="normal")

        elif tipo == "error":
            aviso.set("Aviso: " + str(dato))

    if cambiar_grafico and historial_completo:
        redibujar_grafica(forzar_ejes=False)

    ventana.after(100, actualizar)


# Iniciar el servidor en segundo plano
threading.Thread(
    target=servidor_tcp,
    daemon=True
).start()

ventana.after(100, actualizar)
ventana.mainloop()
