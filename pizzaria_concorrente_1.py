"""
Simulação da Pizzaria Concorrente
Prof. Ludgero Bezerra
Autor: Stenio Lira, Calebe Dantas, Alisk Lopes

Conceitos usados:
- Threads (threading)          -> 8 clientes fazendo pedidos ao mesmo tempo
- Processos (multiprocessing)  -> 2 pizzaiolos isolados, cada um com seu PID
- Fila (multiprocessing.Queue) -> canal seguro entre threads e processos
- Semáforo                     -> limita o forno a no máximo 2 pizzas
- Lock (exclusão mútua)        -> protege a escrita no arquivo pizzas_prontas.txt
"""

import multiprocessing
import os
import platform
import random
import threading
import time
from datetime import datetime

# ---------------- Configurações da simulação ----------------
NUM_CLIENTES = 19
NUM_PIZZAIOLOS = 4
CAPACIDADE_FORNO = 3
TEMPO_FORNO = 4                 # segundos que a pizza fica assando
TEMPO_MONTAGEM = (2, 4)         # montagem da massa: entre 2 e 4 segundos
TEMPO_ENTREGA = (1, 3)          # entrega em trânsito: entre 1 e 3 segundos
SABORES = ["Calabresa", "Marguerita", "Frango com Catupiry", "Quatro Queijos"]
ARQUIVO_PRONTAS = "pizzas_prontas.txt"
FIM_DO_EXPEDIENTE = None        # sinal colocado na fila para o pizzaiolo parar


def log(trava_print, mensagem):
    """Imprime uma linha por vez, sem misturar a saída de threads/processos."""
    with trava_print:
        print(mensagem, flush=True)


# ---------------- 2. Clientes (Threads) ----------------
def cliente(fila_pedidos, contador, trava_balcao, trava_print):
    """Cada cliente chega, escolhe um sabor aleatório e coloca o pedido na fila."""
    time.sleep(random.uniform(0, 1))  # clientes chegam em momentos diferentes
    sabor = random.choice(SABORES)

    # O "balcão" atende um cliente por vez: numerar e enfileirar o pedido
    # acontece sem condição de corrida entre as threads.
    with trava_balcao:
        contador[0] += 1
        numero = contador[0]
        log(trava_print, f"[CLIENTE] Pedido #{numero} ({sabor}) realizado.")
        fila_pedidos.put((numero, sabor))


# ---------------- Entrega (Thread disparada pelo pizzaiolo) ----------------
def entregar(numero, trava_print):
    log(trava_print, f"[ENTREGA] Entregador a caminho da entrega #{numero}...")
    time.sleep(random.uniform(*TEMPO_ENTREGA))
    log(trava_print, f"[CONCLUÍDO] Pedido #{numero} entregue ao cliente!")


# ---------------- 3. Cozinha e Preparo (Processos) ----------------
def pizzaiolo(fila_pedidos, forno, vagas_ocupadas, trava_arquivo, trava_print):
    """Processo pizzaiolo: consome pedidos da fila até receber o sinal de parada."""
    random.seed()  # cada processo com sua própria sequência aleatória
    pid = os.getpid()
    entregas = []

    while True:
        pedido = fila_pedidos.get()          # espera um pedido na fila
        if pedido is FIM_DO_EXPEDIENTE:
            break
        numero, sabor = pedido

        # Montagem da massa
        log(trava_print, f"[PIZZAIOLO {pid}] Iniciando preparo da pizza #{numero}.")
        time.sleep(random.uniform(*TEMPO_MONTAGEM))

        # Forno: o semáforo deixa entrar no máximo CAPACIDADE_FORNO pizzas
        with forno:
            with vagas_ocupadas.get_lock():
                vagas_ocupadas.value += 1
                log(trava_print,
                    f"[FORNO] Pizza #{numero} entrou no forno "
                    f"(Vagas ocupadas no forno: {vagas_ocupadas.value}/{CAPACIDADE_FORNO}).")

            time.sleep(TEMPO_FORNO)

            with vagas_ocupadas.get_lock():
                vagas_ocupadas.value -= 1
                log(trava_print, f"[FORNO] Pizza #{numero} assada e retirada do forno.")

        # Registro no arquivo: só um pizzaiolo escreve por vez (Lock)
        with trava_arquivo:
            with open(ARQUIVO_PRONTAS, "a", encoding="utf-8") as arquivo:
                arquivo.write(
                    f"{datetime.now():%H:%M:%S} | Pedido #{numero} | {sabor} | "
                    f"Pizzaiolo PID {pid}\n"
                )

        # Pizza pronta -> dispara a entrega em uma thread
        entregador = threading.Thread(target=entregar, args=(numero, trava_print))
        entregador.start()
        entregas.append(entregador)

    # O pizzaiolo só encerra depois que todas as suas entregas terminarem
    for entregador in entregas:
        entregador.join()


# ---------------- 1. Gerente (processo principal) ----------------
def main():
    inicio = time.time()
    print("=" * 62)
    print("            SIMULAÇÃO DA PIZZARIA CONCORRENTE")
    print("=" * 62)
    print(f"[GERENTE] PID do gerente: {os.getpid()}")
    print(f"[GERENTE] Arquitetura da máquina: {platform.machine()}")
    print(f"[GERENTE] {NUM_CLIENTES} clientes | {NUM_PIZZAIOLOS} pizzaiolos | "
          f"forno para {CAPACIDADE_FORNO} pizzas")
    print("=" * 62, flush=True)

    # Começa o expediente com o arquivo de registro limpo
    with open(ARQUIVO_PRONTAS, "w", encoding="utf-8") as arquivo:
        arquivo.write("Registro de pizzas prontas\n")

    # Recursos compartilhados
    fila_pedidos = multiprocessing.Queue()
    forno = multiprocessing.Semaphore(CAPACIDADE_FORNO)
    vagas_ocupadas = multiprocessing.Value("i", 0)
    trava_arquivo = multiprocessing.Lock()
    trava_print = multiprocessing.Lock()

    # Inicia os pizzaiolos (processos)
    pizzaiolos = []
    for i in range(NUM_PIZZAIOLOS):
        p = multiprocessing.Process(
            target=pizzaiolo,
            args=(fila_pedidos, forno, vagas_ocupadas, trava_arquivo, trava_print),
            name=f"Pizzaiolo-{i + 1}",
        )
        p.start()
        pizzaiolos.append(p)
        log(trava_print, f"[GERENTE] {p.name} iniciado (PID {p.pid}).")

    # Inicia os clientes (threads), todos ao mesmo tempo
    contador = [0]
    trava_balcao = threading.Lock()
    clientes = [
        threading.Thread(
            target=cliente,
            args=(fila_pedidos, contador, trava_balcao, trava_print),
            name=f"Cliente-{i + 1}",
        )
        for i in range(NUM_CLIENTES)
    ]
    for c in clientes:
        c.start()
    for c in clientes:
        c.join()

    # Todos os pedidos já foram feitos: avisa cada pizzaiolo para encerrar
    for _ in pizzaiolos:
        fila_pedidos.put(FIM_DO_EXPEDIENTE)
    for p in pizzaiolos:
        p.join()

    # Conferência final do arquivo
    with open(ARQUIVO_PRONTAS, encoding="utf-8") as arquivo:
        registradas = sum(1 for linha in arquivo if "Pedido #" in linha)

    print("=" * 62)
    print(f"[GERENTE] Expediente encerrado em {time.time() - inicio:.1f} s.")
    print(f"[GERENTE] {registradas}/{NUM_CLIENTES} pedidos registrados em {ARQUIVO_PRONTAS}.")
    print("=" * 62)


if __name__ == "__main__":
    main()
