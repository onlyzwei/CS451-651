package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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

// Temporizações. Heartbeat <= 10/s
const (
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
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
	defer rf.mu.Unlock() // Garante que o mutex será liberado no retorno da func.

	reply.Term = rf.CurrentTerm // meu termo atual
	reply.VoteGranted = false   // padrão: nega voto

	// 1) Se o termo do candidato é menor, recusa.
	if args.Term < rf.CurrentTerm {
		return
	}

	// 2) Viu termo maior?
	if args.Term > rf.CurrentTerm {
		rf.becomeFollowerLocked(args.Term) // atualiza termo, limpa voto e vira follower
	}

	// 3) Concede voto se ainda não votou no termo ou já votou no próprio candidato
	//    e se o log do candidato é pelo menos tão atualizado quanto o meu.
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && rf.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		rf.VotedFor = args.CandidateId // concede voto
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm // atualiza termo no reply
		// Concedeu voto → reseta timeout para evitar eleições desnecessárias.
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
func (rf *Raft) electionLoop() {
	rf.mu.Lock()
	rf.resetElectionTimerLocked() // inicia timer de eleição
	rf.mu.Unlock()

	for {
		// Bloqueia até timeout da eleição.
		<-rf.ElectionTimer.C
		if atomic.LoadInt32(&rf.dead) == 1 { // morto
			return
		}

		// Se não é líder, vira candidato e tenta eleição.
		rf.mu.Lock()
		if rf.State != StateLeader {
			rf.startElectionLocked()
			// Ao terminar startElectionLocked, o nó pode ter virado líder ou follower.
			// Em qualquer caso reagenda o próximo timeout.
			rf.resetElectionTimerLocked()
		} else {
			// Se já é líder, apenas reagenda o timer para não disparar eleições enquanto líder.
			rf.resetElectionTimerLocked()
		}
		rf.mu.Unlock()
	}
}

// Dispara uma rodada de eleição: incrementa termo, vota em si, pede votos e conta maioria.
// Deve ser chamada com rf.mu travado.
func (rf *Raft) startElectionLocked() {
	termStarted := rf.CurrentTerm + 1
	rf.CurrentTerm = termStarted
	rf.State = StateCandidate
	rf.VotedFor = rf.me

	// Dados do meu log para a regra de “log atualizado”.
	lastIdx, lastTerm := rf.lastIndexAndTermLocked()
	// O que seria o lastIdx e lastTerm do candidato?
	// lastIdx: índice do último log (len(rf.Log)-1)
	// lastTerm: termo do último log (rf.Log[lastIdx].Term)

	votes := 1               // voto em si
	nPeers := len(rf.peers)  // número total de peers (incluindo o próprio)
	majority := nPeers/2 + 1 // maioria simples

	// Copia valores imutáveis para usar fora do lock.
	me := rf.me // O que é rf.me? É o índice deste peer na lista de peers.

	args := &RequestVoteArgs{
		Term:         termStarted,
		CandidateId:  me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Envia RequestVote em paralelo.
	rf.mu.Unlock()
	var wg sync.WaitGroup               // para aguardar todas as goroutines
	voteCh := make(chan bool, nPeers-1) // canal para votos recebidos
	termCh := make(chan int, nPeers-1)  // canal para termos maiores recebidos

	// Envia RequestVote para todos os outros peers.
	for i := 0; i < nPeers; i++ {
		// pula si mesmo
		if i == me {
			continue
		}
		// dispara goroutine para enviar RPC
		// O que é wg, e porque eu Adiciono 1 nele?
		// wg é um WaitGroup que usamos para esperar que todas as goroutines terminem antes de prosseguir.
		// Adicionamos 1 para cada goroutine que iniciamos, para que possamos esperar por todas elas mais tarde.
		wg.Add(1)
		// dispara a goroutine para o peer i, porque cada peer deve ser tratado de forma independente.
		go func(peer int) {
			defer wg.Done()            // sinaliza que esta goroutine terminou ao sair
			var reply RequestVoteReply // estrutura para armazenar a resposta

			// Se a RPC for bem-sucedida, envia o resultado para os canais apropriados.
			if rf.sendRequestVote(peer, args, &reply) {
				termCh <- reply.Term        // envia termo recebido
				voteCh <- reply.VoteGranted // envia voto recebido
			}
		}(i) // passa i como argumento para evitar captura de variável
	}
	wg.Wait() // aguarda todas as goroutines terminarem
	close(voteCh)
	close(termCh)
	rf.mu.Lock()

	// Se durante as RPCs alguém nos informou termo maior, atualiza e recua para follower.
	for t := range termCh {
		if t > rf.CurrentTerm {
			rf.becomeFollowerLocked(t) // atualiza termo e vira follower, pq atualizar o termo?
			// Porque se outro nó tem um termo maior, significa que
			// ele está mais atualizado
		}
	}

	// Se já não é mais candidato (porque recebeu heartbeat ou termo maior), aborta.
	if rf.State != StateCandidate || rf.CurrentTerm != termStarted {
		return
	}

	// Soma votos recebidos nesta rodada.
	for v := range voteCh {
		if v {
			votes++
		}
	}

	// Ganhou?
	if votes >= majority {
		rf.State = StateLeader
		// Como líder, começa a enviar heartbeats periódicos.
		go rf.heartbeatLoop(termStarted)
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
		// Se mudou de estado ou termo, para heartbeats.
		if rf.State != StateLeader || rf.CurrentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}
		// Envia um heartbeat para todos os peers.
		rf.sendHeartbeatsLocked()
		rf.mu.Unlock()

		<-t.C
	}
}

// Envia heartbeats (AppendEntries com Entries vazias). Reage a termos maiores nas respostas.
func (rf *Raft) sendHeartbeatsLocked() {
	n := len(rf.peers)
	me := rf.me
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.CommitIndex,
	}
	currTerm := rf.CurrentTerm

	rf.mu.Unlock()
	var wg sync.WaitGroup
	newTermCh := make(chan int, n-1)

	for i := 0; i < n; i++ {
		if i == me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			var reply AppendEntriesReply
			ok := rf.peers[peer].Call("Raft.AppendEntries", args, &reply)
			if ok {
				if reply.Term > currTerm {
					newTermCh <- reply.Term
				}
			}
		}(i)
	}
	wg.Wait()
	close(newTermCh)
	rf.mu.Lock()

	// Se alguém respondeu com termo maior, atualiza e recua a follower.
	for nt := range newTermCh {
		if nt > rf.CurrentTerm {
			rf.becomeFollowerLocked(nt)
		}
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
	// Semente aleatória para evitar timeouts iguais em todos os nós.
	rand.Seed(time.Now().UnixNano())

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
