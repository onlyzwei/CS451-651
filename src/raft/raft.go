package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var seedOnce sync.Once // Garante que a semente aleatória seja inicializada apenas uma vez.

// Mensagem entregue ao serviço quando entradas são aplicadas (não usado em 2A).
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// Estados do servidor.
const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

// Temporizações.
const (
	// Aumente estes valores para evitar eleições desnecessárias em máquinas mais lentas
	ElectionTimeoutMin = 400 * time.Millisecond
	ElectionTimeoutMax = 800 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond // Mantido em 10 req/s
)

// Entrada de log (em 2A o conteúdo não é usado, mas precisamos do termo).
type LogEntry struct {
	Term    int
	Command interface{}
}

// Estrutura principal de um peer Raft.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	// Estado persistente (não usado em 2A):
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry // Convenção: índice começa em 1; posição 0 é sentinela.

	// Estado volátil:
	CommitIndex int
	LastApplied int

	// Estado volátil só no líder (não usado em 2A):
	NextIndex  []int
	MatchIndex []int

	// Auxiliares:
	State         int
	ElectionTimer *time.Timer
}

// GetState retorna o termo atual e se este nó se vê como líder.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()                           // Garante que o mutex será liberado no retorno da func.
	return rf.CurrentTerm, rf.State == StateLeader // retorna (term,isLeader)
}

// não usado em 2A
func (rf *Raft) persist()                {}
func (rf *Raft) readPersist(data []byte) {}

// ======= RPCs =======

// Args/Reply do RequestVote.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// Args/Reply do AppendEntries (2A usa como heartbeat: Entries vazias).
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Implementação de RequestVote, incluindo atualização de termo e regra de “log atualizado”.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1) Se o termo do candidato é menor, recusa imediatamente.
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	// 2) Se viu termo maior, atualiza.
	if args.Term > rf.CurrentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// Configure o termo de resposta SEMPRE com o termo mais atualizado
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	// 3) Verifica se pode conceder o voto
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && rf.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
	}
}

// Implementação de AppendEntries mínimo para 2A (heartbeat).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // Garante que o mutex será liberado no retorno da func.

	reply.Success = false       // padrão: falha
	reply.Term = rf.CurrentTerm // meu termo atual

	// 1) Termo antigo? Recusa.
	if args.Term < rf.CurrentTerm { // termo desatualizado
		return
	}

	// 2) Termo novo ou igual: atualiza termo se preciso, vira follower e aceita heartbeat.
	if args.Term > rf.CurrentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// Ao receber heartbeat válido, permanece/volta a follower.
	if rf.State != StateFollower {
		rf.State = StateFollower
	}
	// Heartbeat aceito.
	reply.Success = true
	reply.Term = rf.CurrentTerm
	// Heartbeat válido → reseta timeout de eleição.
	rf.resetElectionTimerLocked()
}

// Envio de RequestVote (RPC cliente).
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// ======= Temporizador de eleição =======

func randElectionTimeout() time.Duration {
	d := ElectionTimeoutMax - ElectionTimeoutMin
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(d)))
}

// Deve ser chamada com rf.mu travado.
func (rf *Raft) resetElectionTimerLocked() {
	if rf.ElectionTimer == nil { // primeira vez
		rf.ElectionTimer = time.NewTimer(randElectionTimeout()) // inicia o timer
		return
	}
	// O que significa !rf.ElectionTimer.Stop()? Significa que o timer já expirou e o canal C já recebeu um valor.
	// Nesse caso, precisamos drenar o canal para evitar que um valor antigo permaneça lá e
	// cause um disparo inesperado no futuro.
	// este valor antigo poderia fazer com que o timer disparasse imediatamente após ser resetado,
	// obs: este valor em C é usado apenas para notificar que o timer expirou.
	if !rf.ElectionTimer.Stop() {
		select {
		case <-rf.ElectionTimer.C:
		default:
		}
	}
	rf.ElectionTimer.Reset(randElectionTimeout()) // reseta com novo timeout aleatório
}

// ======= Laços de fundo =======

// Laço principal: controla tempo de eleição. Ao expirar, inicia uma eleição.
// Também dispara e mantém heartbeats quando torna-se líder.
// Laço principal: controla tempo de eleição. Ao expirar, inicia uma eleição.
func (rf *Raft) electionLoop() {
	rf.mu.Lock()
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	for {
		<-rf.ElectionTimer.C
		if atomic.LoadInt32(&rf.dead) == 1 {
			return
		}

		rf.mu.Lock()
		// CORREÇÃO CRÍTICA: Se já for líder, NÃO inicia nova eleição.
		// Apenas reagenda o timer para manter o ciclo vivo caso deixe de ser líder.
		if rf.State != StateLeader {
			rf.startElectionLocked()
		}
		rf.resetElectionTimerLocked()
		rf.mu.Unlock()
	}
}

// Dispara uma rodada de eleição: incrementa termo, vota em si, pede votos e conta maioria.
// Deve ser chamada com rf.mu travado.
// Dispara uma rodada de eleição de forma não-bloqueante.
// Dispara uma rodada de eleição de forma não-bloqueante.
// Deve ser chamada com rf.mu travado.
func (rf *Raft) startElectionLocked() {
	termStarted := rf.CurrentTerm + 1
	rf.CurrentTerm = termStarted
	rf.State = StateCandidate
	rf.VotedFor = rf.me

	lastIdx, lastTerm := rf.lastIndexAndTermLocked()
	me := rf.me
	votes := 1 // voto em si mesmo
	nPeers := len(rf.peers)
	majority := nPeers/2 + 1

	args := &RequestVoteArgs{
		Term:         termStarted,
		CandidateId:  me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	for i := 0; i < nPeers; i++ {
		if i == me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Se o estado mudou (ex: recebeu heartbeat e virou follower) ou o termo mudou, aborta.
				if rf.State != StateCandidate || rf.CurrentTerm != termStarted {
					return
				}

				if reply.Term > rf.CurrentTerm {
					rf.becomeFollowerLocked(reply.Term)
					return
				}

				if reply.VoteGranted {
					votes++
					if votes == majority {
						// Atingiu maioria: vira líder e inicia heartbeats imediatamente.
						rf.State = StateLeader
						go rf.heartbeatLoop(termStarted)
					}
				}
			}
		}(i)
	}
}

// Laço de heartbeats: enquanto líder no mesmo termo, envia batimentos periódicos.
func (rf *Raft) heartbeatLoop(leaderTerm int) {
	t := time.NewTicker(HeartbeatInterval)
	defer t.Stop()
	for {
		if atomic.LoadInt32(&rf.dead) == 1 {
			return
		}
		rf.mu.Lock()
		if rf.State != StateLeader || rf.CurrentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}
		rf.sendHeartbeatsLocked() // <--- Apenas chamada direta, sem 'if' ou ':='
		rf.mu.Unlock()

		<-t.C
	}
}

// Envia heartbeats (AppendEntries com Entries vazias). Reage a termos maiores nas respostas.
// retorna quantos seguidores responderam no MESMO termo atual
// Envia heartbeats para todos os peers de forma não-bloqueante.
// Deve ser chamada com rf.mu travado.
func (rf *Raft) sendHeartbeatsLocked() {
	me := rf.me
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     me,
		PrevLogIndex: 0,   // Simplificação para 2A
		PrevLogTerm:  0,   // Simplificação para 2A
		Entries:      nil, // Heartbeat
		LeaderCommit: rf.CommitIndex,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		go func(peer int) {
			var reply AppendEntriesReply
			if rf.peers[peer].Call("Raft.AppendEntries", args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// Verifica termo maior na resposta
				if reply.Term > rf.CurrentTerm {
					rf.becomeFollowerLocked(reply.Term)
				}
			}
		}(i)
	}
}

// ======= Utilitários internos =======

// Deve ser chamada com rf.mu travado: transição para follower com novo termo.
func (rf *Raft) becomeFollowerLocked(newTerm int) {
	rf.CurrentTerm = newTerm
	rf.State = StateFollower
	rf.VotedFor = -1
}

// Comparação “log do candidato é pelo menos tão atualizado quanto o meu”.
// Em 2A o log é trivial, mas seguimos a regra da Figura 2:
// 1) maior LastLogTerm vence; 2) em empate de termo, maior LastLogIndex vence.
func (rf *Raft) candidateUpToDateLocked(cLastIdx, cLastTerm int) bool {
	myLastIdx, myLastTerm := rf.lastIndexAndTermLocked()
	if cLastTerm != myLastTerm {
		return cLastTerm > myLastTerm
	}
	return cLastIdx >= myLastIdx
}

// Retorna último índice e termo do meu log. Deve ser chamada com rf.mu travado.
func (rf *Raft) lastIndexAndTermLocked() (int, int) {
	lastIdx := len(rf.Log) - 1
	if lastIdx >= 0 {
		return lastIdx, rf.Log[lastIdx].Term
	}
	return 0, 0
}

// ======= API não usada em 2A =======

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	return -1, -1, false
}

// Encerra a instância: marca como morta e dispara o timer para acordar laços bloqueados.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.ElectionTimer != nil {
		if !rf.ElectionTimer.Stop() {
			select {
			case <-rf.ElectionTimer.C:
			default:
			}
		}
		rf.ElectionTimer.Reset(0)
	}
}

// Criação de um novo peer Raft. Deve retornar rápido e iniciar goroutines de fundo.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	seedOnce.Do(func() { rand.Seed(time.Now().UnixNano()) }) // <-- semear uma vez só

	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         make([]LogEntry, 1), // índice 0 é sentinela; primeiro índice real é 1.

		CommitIndex: 0,
		LastApplied: 0,

		State: StateFollower,
	}

	// Restaura estado persistido se houver (só usado em 2C; aqui não altera nada).
	rf.readPersist(persister.ReadRaftState())

	// Inicia o laço de eleição.
	go rf.electionLoop()

	return rf
}
