// eleição de líder e heartbeats (AppendEntries vazio).
package raft

import (
	"labrpc"      // Framework de RPC do lab; o testador injeta atrasos, perdas, reordenação (não modificar).
	"math/rand"   // Usado para sortear timeouts de eleição (randomização evita empate).
	"sync"        // Mutex para proteger estado compartilhado do peer.
	"sync/atomic" // Atômicos para flags de vida/morte do peer.
	"time"        // Timers
)

// seedOnce garante que semeamos o gerador de números aleatórios uma única vez
// por processo. Evita que diferentes goroutines chamem rand.Seed repetidamente.
var seedOnce sync.Once

// ApplyMsg é a mensagem que um peer Raft envia ao serviço quando uma entrada
// é "committed" e aplicada. No 2A não há aplicação de comandos, mas mantemos
// a estrutura para compatibilidade com a interface exigida.
type ApplyMsg struct {
	Index       int         // Índice da entrada aplicada.
	Command     interface{} // Comando aplicado à máquina de estados.
	UseSnapshot bool        // Sinaliza uso de snapshot (não usado no 2A).
	Snapshot    []byte      // Dados do snapshot (não usado no 2A).
}

// Estados do servidor conforme o modelo do Raft: follower, candidate, leader.
// Esses estados determinam o comportamento no loop principal (Figura 2).
const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

// Parâmetros de temporização. O testador limita heartbeats a 10/s,
// então usamos 100 ms para batimentos e timeouts de eleição mais longos,
// dentro da janela recomendada pelo enunciado (eleição < 5 s)
const (
	// Sugestão do enunciado: aumentar para evitar eleições desnecessárias em máquinas lentas.
	ElectionTimeoutMin = 400 * time.Millisecond // limite inferior do sorteio.
	ElectionTimeoutMax = 800 * time.Millisecond // limite superior do sorteio.
	HeartbeatInterval  = 100 * time.Millisecond // 10 Hz para atender o testador.
)

// LogEntry representa uma entrada no log replicado.
// No 2A só precisamos do Termo para implementar a regra "log up-to-date".
type LogEntry struct {
	Term    int         // Termo em que a entrada foi criada pelo líder.
	Command interface{} // Comando da máquina de estados (não usado no 2A).
}

// Estrutura principal de um peer Raft.
// Todos os campos acessados por múltiplas goroutines são protegidos por rf.mu.
type Raft struct {
	// ---------- Infraestrutura ----------
	mu        sync.Mutex          // Mutex para serializar acesso ao estado.
	peers     []*labrpc.ClientEnd // Endpoints RPC de todos os peers (eu incluso).
	persister *Persister          // Persistência (usada em 2C+; mantemos para compatibilidade).
	me        int                 // Meu índice no slice peers.
	dead      int32               // Flag atômica para shutdown cooperativo.

	// ---------- Estado persistente (Figura 2) ----------
	// No 2A já mantemos esses campos, embora persistência efetiva só apareça em 2C.
	CurrentTerm int        // Maior termo visto por este servidor; monotônico.
	VotedFor    int        // CandidateId para quem votei no CurrentTerm; -1 se ninguém.
	Log         []LogEntry // Log de entradas; convenção: index começa em 1. Posição 0 é sentinela.

	// ---------- Estado volátil (Figura 2) ----------
	CommitIndex int // Maior índice "committed" conhecido (0 no 2A).
	LastApplied int // Maior índice aplicado à máquina de estados (0 no 2A).

	// ---------- Estado volátil do líder (Figura 2) ----------
	// Só será usado a partir de 2B, mas definimos por completude.
	NextIndex  []int // Próximo índice a enviar para cada seguidor.
	MatchIndex []int // Maior índice replicado conhecido em cada seguidor.

	// ---------- Auxiliares locais ----------
	State         int         // Um dos valores StateFollower/StateCandidate/StateLeader.
	ElectionTimer *time.Timer // Timer para disparar eleições ao expirar.
}

// GetState retorna (term, isLeader). É chamado pelo testador.
// Deve ser seguro para concorrência, então pegamos o lock.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // Libera lock ao sair, mesmo com panic/return antecipado.
	return rf.CurrentTerm, rf.State == StateLeader
}

// Persistência de estado (usada em 2C+). No 2A são stubs para compilar.
func (rf *Raft) persist()                {}
func (rf *Raft) readPersist(data []byte) {}

// ================================ RPCs =====================================

// RequestVoteArgs carrega os argumentos do pedido de voto (Figura 2).
// Inclui o "último índice/termo do log" para a regra de "log atualizado".
type RequestVoteArgs struct {
	Term         int // Termo do candidato.
	CandidateId  int // Id do candidato que solicita o voto.
	LastLogIndex int // Último índice do log do candidato.
	LastLogTerm  int // Termo da última entrada do log do candidato.
}

// RequestVoteReply informa o termo do receptor e se o voto foi concedido.
type RequestVoteReply struct {
	Term        int  // Termo do receptor, para o candidato atualizar-se se estiver defasado.
	VoteGranted bool // true se voto concedido neste termo.
}

// AppendEntriesArgs são os argumentos do heartbeat/replicação (Figura 2).
// No 2A só usamos Term, LeaderId e LeaderCommit; Entries permanece vazio.
type AppendEntriesArgs struct {
	Term         int        // Termo do líder remetente.
	LeaderId     int        // Id do líder, usado para redirecionar clientes.
	PrevLogIndex int        // Índice anterior às novas entradas (não usado no 2A).
	PrevLogTerm  int        // Termo da entrada em PrevLogIndex (não usado no 2A).
	Entries      []LogEntry // Entradas a replicar; vazio significa heartbeat.
	LeaderCommit int        // Índice commitado no líder.
}

// AppendEntriesReply retorna o termo do seguidor e se a operação teve sucesso.
type AppendEntriesReply struct {
	Term    int  // Termo atual do seguidor.
	Success bool // true se cheque de consistência passou; no 2A sempre true quando termo OK.
}

// ====================== Implementações dos handlers RPC =====================

// RequestVote implementa as regras de voto do Raft:
// 1) Rejeita candidatos com termo menor;
// 2) Atualiza para termos maiores e vira follower;
// 3) Concede voto se ainda não votou no termo e se o log do candidato é "pelo menos tão atualizado".
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()         // Serializa com outros acessos ao estado.
	defer rf.mu.Unlock() // Garante liberação do lock.

	// Regra 1: candidato com termo menor é rejeitado imediatamente.
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm // Retorna meu termo para o candidato atualizar-se.
		return
	}

	// Regra 2: se vejo um termo maior, atualizo meu CurrentTerm e viro follower.
	if args.Term > rf.CurrentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// Preenche sempre o Term da resposta com meu CurrentTerm já atualizado.
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false // default: não conceder, até provar que pode.

	// Regra 3: posso votar se ainda não votei neste termo ou se estou repetindo o mesmo voto,
	// e se o log do candidato é pelo menos tão atualizado quanto o meu (Figura 2 §5.4).
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		rf.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {

		rf.VotedFor = args.CandidateId // Registra voto no termo corrente.
		reply.VoteGranted = true       // Concede o voto.
		rf.resetElectionTimerLocked()  // Reseta o timer para não disparar eleição indevida.
	}
}

// AppendEntries mínimo para 2A: usado como heartbeat periódico do líder.
// Regras aplicadas:
//  1. Rejeita termos antigos;
//  2. Para termos ≥ currentTerm, atualiza o termo se necessário, mantém-se/volta a follower,
//     aceita o heartbeat e reseta o timer de eleição.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // Protege acesso ao estado.

	reply.Success = false       // Default: falha.
	reply.Term = rf.CurrentTerm // Ecoa meu termo atual.

	// Regra 1: se o líder tem termo desatualizado, rejeita.
	if args.Term < rf.CurrentTerm {
		return // Success permanece false.
	}

	// Regra 2: termo novo ou igual → atualiza se necessário e age como follower.
	if args.Term > rf.CurrentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// Mesmo se eu estivesse candidate/leader, um heartbeat válido me leva a follower.
	if rf.State != StateFollower {
		rf.State = StateFollower
	}

	// Este é um heartbeat válido: aceita.
	reply.Success = true
	reply.Term = rf.CurrentTerm

	// Heartbeat recebido → não iniciar eleição enquanto o líder estiver ativo.
	rf.resetElectionTimerLocked()
}

// sendRequestVote é o cliente RPC para enviar RequestVote a um peer.
// Isola a chamada para facilitar testes e troca de implementação.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// ============================== Temporizador ================================

// randElectionTimeout sorteia um timeout no intervalo [min, max).
// Randomização reduz colisões de eleição entre vários seguidores. :contentReference[oaicite:6]{index=6}
func randElectionTimeout() time.Duration {
	d := ElectionTimeoutMax - ElectionTimeoutMin
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(d)))
}

// resetElectionTimerLocked reinicia o ElectionTimer com um novo timeout aleatório.
// Precisa drenar o canal do timer se ele já disparou, para evitar disparo ilegítimo após reset.
func (rf *Raft) resetElectionTimerLocked() {
	if rf.ElectionTimer == nil { // Inicialização lazily: cria timer na primeira chamada.
		rf.ElectionTimer = time.NewTimer(randElectionTimeout()) // Começa a contar imediatamente.
		return
	}
	// Stop retorna false se o timer já havia expirado e havia um valor pendente em C.
	// Nesse caso, drenamos C para limpar o evento antigo antes de resetar.
	if rf.ElectionTimer.Stop() == false {
		select {
		case <-rf.ElectionTimer.C: // Consome o tick que sobrou após ser chamado resetElectionTimerLocked,
			// evitando que ele dispare inesperadamente depois do reset.
			// pois estou consumindo o valor do canal. basicamente uma mensagem que precisa
			// ser consumida para evitar que o timer dispare inesperadamente depois do reset.
		default: // Nada a drenar.
		}
	}
	// Reinicia com um novo valor aleatório, evitando sincronização entre peers.
	rf.ElectionTimer.Reset(randElectionTimeout())
}

// ============================== Laços de fundo ==============================

// electionLoop é o loop principal que vigia o ElectionTimer.
// Quando o timer expira, inicia uma eleição se eu não for líder.
// Continua enquanto o peer não for "morto" via Kill().
func (rf *Raft) electionLoop() {
	// Inicializa o timer uma vez antes do loop.
	rf.mu.Lock()
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	for {
		<-rf.ElectionTimer.C                 // Bloqueia até o timeout expirar.
		if atomic.LoadInt32(&rf.dead) == 1 { // Encerramento solicitado?
			return
		}

		rf.mu.Lock()
		// Se já sou líder, não inicio uma nova eleição; apenas reprogramo o timer.
		// Isso evita tempestades de eleição enquanto em liderança estável.
		if rf.State != StateLeader {
			rf.startElectionLocked()
		}
		rf.resetElectionTimerLocked()
		rf.mu.Unlock()
	}
}

// startElectionLocked inicia uma rodada de eleição:
// incrementa o termo, vira candidate, vota em si, envia RequestVote,
// e promove a líder quando atinge maioria. Não bloqueia o loop.
func (rf *Raft) startElectionLocked() {
	termStarted := rf.CurrentTerm + 1 // Próximo termo.
	rf.CurrentTerm = termStarted
	rf.State = StateCandidate
	rf.VotedFor = rf.me // Vota em si mesmo por definição.

	// Captura meu último índice/termo para a regra de "log atualizado".
	lastIdx, lastTerm := rf.lastIndexAndTermLocked()
	me := rf.me
	votes := 1 // Já conto meu próprio voto.
	nPeers := len(rf.peers)
	majority := nPeers/2 + 1 // Maioria simples: floor(N/2)+1.

	// Prepara os argumentos comuns a todos os pedidos de voto.
	args := &RequestVoteArgs{
		Term:         termStarted,
		CandidateId:  me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Dispara pedidos de voto para todos os outros peers em goroutines.
	for i := 0; i < nPeers; i++ {
		if i == me {
			continue // Não envio para mim mesmo.
		}
		go func(peer int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, args, &reply) { // RPC retornou.
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Aborta contagem se: deixei de ser candidate, ou meu termo mudou (nova eleição começou).
				if rf.State != StateCandidate || rf.CurrentTerm != termStarted {
					return
				}

				// Se recebo uma resposta com termo maior, atualizo e viro follower imediatamente.
				if reply.Term > rf.CurrentTerm {
					rf.becomeFollowerLocked(reply.Term)
					return
				}

				// Soma voto concedido; ao atingir maioria, promove a líder e inicia heartbeats.
				if reply.VoteGranted {
					votes++
					if votes == majority {
						rf.State = StateLeader           // Torna-se líder deste termo.
						go rf.heartbeatLoop(termStarted) // Começa a enviar heartbeats periódicos.
					}
				}
			}
		}(i)
	}
}

// heartbeatLoop envia AppendEntries vazios a todos os peers a cada HeartbeatInterval,
// enquanto permanecer líder no mesmo termo. Sai se perder a liderança ou for morto.
func (rf *Raft) heartbeatLoop(leaderTerm int) {
	t := time.NewTicker(HeartbeatInterval) // Ticker periódico.
	defer t.Stop()                         // Se função falhar, garante que o ticker seja parado.
	for {
		if atomic.LoadInt32(&rf.dead) == 1 { // Encerramento solicitado.
			return
		}
		rf.mu.Lock()
		// Interrompe se não sou mais líder ou se o termo mudou (nova eleição ocorreu).
		if rf.State != StateLeader || rf.CurrentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}
		rf.sendHeartbeatsLocked() // Envia um round de heartbeats.
		rf.mu.Unlock()

		<-t.C // Espera o próximo tick.
	}
}

// sendHeartbeatsLocked envia AppendEntries com Entries vazias para todos os seguidores.
// É chamado somente quando rf.State == StateLeader e com o lock segurado.
// Reage a respostas com termo maior rebaixando-se a follower.
func (rf *Raft) sendHeartbeatsLocked() {
	me := rf.me
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm, // Meu termo atual como líder.
		LeaderId:     me,             // Meu id.
		PrevLogIndex: 0,              // No 2A, simplificamos; consistência de log entra no 2B.
		PrevLogTerm:  0,              // Idem.
		Entries:      nil,            // Vazio = heartbeat.
		LeaderCommit: rf.CommitIndex, // Compromissos conhecidos; ainda 0 no 2A.
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue // Não envio para mim.
		}
		go func(peer int) {
			var reply AppendEntriesReply
			if rf.peers[peer].Call("Raft.AppendEntries", args, &reply) { // RPC síncrono do labrpc.
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// Se algum seguidor responde com termo maior, estou defasado: rebaixo a follower.
				if reply.Term > rf.CurrentTerm {
					rf.becomeFollowerLocked(reply.Term)
				}
			}
		}(i)
	}
}

// ============================ Utilidades internas ===========================

// becomeFollowerLocked atualiza o CurrentTerm, zera VotedFor e define estado follower.
// Deve ser chamada com o mutex travado. Implementa a regra "se ver termo maior, torne-se follower".
func (rf *Raft) becomeFollowerLocked(newTerm int) {
	rf.CurrentTerm = newTerm
	rf.State = StateFollower
	rf.VotedFor = -1 // A partir de agora, ainda não votei neste novo termo.
}

// candidateUpToDateLocked implementa a comparação "log do candidato é pelo menos
// tão atualizado quanto o meu", termo mais alto vence; em empate,
// maior índice vence. Isso evita eleger líder sem entradas já commitadas.
func (rf *Raft) candidateUpToDateLocked(cLastIdx, cLastTerm int) bool {
	myLastIdx, myLastTerm := rf.lastIndexAndTermLocked()
	if cLastTerm != myLastTerm {
		return cLastTerm > myLastTerm
	}
	return cLastIdx >= myLastIdx
}

// lastIndexAndTermLocked retorna o último índice/termo do meu log.
// Usamos um sentinela na posição 0 para facilitar índices 1-based.
func (rf *Raft) lastIndexAndTermLocked() (int, int) {
	lastIdx := len(rf.Log) - 1
	if lastIdx >= 0 {
		return lastIdx, rf.Log[lastIdx].Term
	}
	return 0, 0 // Log vazio: índice 0 com termo 0 (sentinela).
}

// ============================== TESTES =============================

// No 2A não aplicamos nem replicamos,
// então retornamos valores nulos. Mantemos a assinatura exigida pela interface do lab.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	return -1, -1, false
}

// Kill encerra a instância de forma cooperativa:
// marca dead=1; acorda loops bloqueados drenanando e resetando o timer a zero.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.ElectionTimer != nil {
		// Para o timer; se já expirou, drena o canal para limpar tick pendente.
		if !rf.ElectionTimer.Stop() {
			select {
			case <-rf.ElectionTimer.C:
			default:
			}
		}
		// Reset(0) acorda quem estiver esperando no canal, ajudando o loop a terminar.
		rf.ElectionTimer.Reset(0)
	}
}

// Make constrói um novo peer Raft.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Garante que rand.Seed seja chamado uma única vez no processo.
	seedOnce.Do(func() { rand.Seed(time.Now().UnixNano()) })

	// Instancia com valores padrão seguros para 2A.
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		CurrentTerm: 0,                   // Inicia no termo 0.
		VotedFor:    -1,                  // Não votei em ninguém ainda neste termo.
		Log:         make([]LogEntry, 1), // Índice 0 é sentinela. Primeiro índice real será 1.

		CommitIndex: 0,
		LastApplied: 0,

		State: StateFollower, // Inicia como follower
	}

	// Restaura estado persistido (só terá efeito em 2C; aqui é no-op).
	rf.readPersist(persister.ReadRaftState())

	// Inicia o laço que cuida de timeouts e eleições.
	go rf.electionLoop()

	return rf
}
