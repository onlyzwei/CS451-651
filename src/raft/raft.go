package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//
//	create a new Raft server.
//
// rf.Start(command interface{}) (index, term, isleader)
//
//	start agreement on a new log entry
//
// rf.GetState() (term, isLeader)
//
//	ask a Raft for its current term, and whether it thinks it is leader
//
// ApplyMsg
//
//	each time a new entry is committed to the log, each Raft peer
//	should send an ApplyMsg to the service (or tester)
//	in the same server.
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Constantes para os estados do servidor
const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

// Constantes de tempo
const (
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
)

// Estrutura da LogEntry
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Mutex para proteger o acesso concorrente a este estado
	peers     []*labrpc.ClientEnd // Conexoes RPC para todos os servidores
	persister *Persister          // Objeto para armazenar e ler o estado persistente
	me        int                 // Indice deste servidor em peers[]
	dead      int32               // set by Kill()

	// Estado Persistente 	(em todos os servidores)
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// Estado Volatil 		(em todos os servidores)
	CommitIndex int
	LastApplied int

	// Estado Volatil		(apenas lideres)
	NextIndex  []int
	MatchIndex []int

	// Estado Interno (nao esta na figura 2, mas eh necessario)
	State         int         // (Follower, Candidate, Leader)
	ElectionTimer *time.Timer // Timer que dispara a eleicao

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.CurrentTerm
	isleader := rf.State == StateLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type RequestVoteArgs struct {
	Term         int // Termo do candidato
	CandidateId  int // ID do cand. que solicitou o voto
	LastLogIndex int // Indice da ultima LogEntry do candidato
	LastLogTerm  int // Termo da ultima LogEntry do candidato
}

type RequestVoteReply struct {
	Term        int  // Termo atual do recipiente (para o candidato se atualizar)
	VoteGranted bool // Se true, então o candidato recebeu o voto
}

type AppendEntriesArgs struct {
	Term         int        // Termo do lider
	LeaderId     int        // ID do leader (para o seguidor redirecionar clientes)
	PrevLogIndex int        // Indice da LogEntry imediatamente anterior as novas
	PrevLogTerm  int        // Termo da entrada PrevLogIndex
	Entries      []LogEntry // Entradas de log (vazio para heartbeat)
	LeaderCommit int        // Indice de "commit" do lider

}

type AppendEntriesReply struct {
	Term    int  // Termo atual (para o lider se atualizar)
	Success bool // Se true, entao o seguidor contem a entrada PrevLogIndex/PrevLogTerm
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

// Gera timeout aleatório no intervalo [min, max).
func randElectionTimeout() time.Duration {
	d := ElectionTimeoutMax - ElectionTimeoutMin
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(d)))
}

// Deve ser chamada com rf.mu travado.
func (rf *Raft) resetElectionTimerLocked() {
	if rf.ElectionTimer == nil {
		rf.ElectionTimer = time.NewTimer(randElectionTimeout())
		return
	}
	if !rf.ElectionTimer.Stop() {
		select {
		case <-rf.ElectionTimer.C:
		default:
		}
	}
	rf.ElectionTimer.Reset(randElectionTimeout())
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill sinaliza que este servidor Raft não será mais usado.
// Define o flag `dead` e força o timer a disparar imediatamente,
// permitindo que loops como electionLoop() saiam de forma limpa.
func (rf *Raft) Kill() {
	// Marca este servidor como "morto".
	atomic.StoreInt32(&rf.dead, 1)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.ElectionTimer != nil {
		// Para o timer. Se ele já tiver disparado, drena o canal.
		if !rf.ElectionTimer.Stop() {
			select {
			case <-rf.ElectionTimer.C: // Drena o canal se necessário.
			default:
			}
		}

		// Reseta o timer para 0, o que faz ele disparar imediatamente.
		// Isso acorda o electionLoop() caso ele esteja bloqueado em `<-rf.ElectionTimer.C`.
		rf.ElectionTimer.Reset(0)
	}
}

// electionLoop roda em background e é responsável por controlar o timer de eleição.
// Quando o timer expira, o servidor (se não for líder) se torna candidato e inicia um novo termo.
// Essa função roda até que o servidor seja "morto" com Kill().
func (rf *Raft) electionLoop() {
	// Inicializa o timer de eleição com um valor aleatório entre ElectionTimeoutMin e ElectionTimeoutMax.
	rf.mu.Lock()
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	for {
		// Espera o timer expirar. Essa linha BLOQUEIA até que o timer dispare.
		<-rf.ElectionTimer.C

		// Verifica se o servidor foi "morto" (Kill() chamado). Se sim, encerra a goroutine.
		if atomic.LoadInt32(&rf.dead) == 1 {
			return
		}

		// Caso o servidor ainda esteja ativo, verifica seu estado.
		rf.mu.Lock()
		if rf.State != StateLeader {
			// Se não for líder, inicia o processo de eleição (por enquanto só muda o estado e termo).
			rf.State = StateCandidate
			rf.CurrentTerm++
		}

		// Reinicia o timer de eleição com um novo valor aleatório.
		rf.resetElectionTimerLocked()
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.CurrentTerm = 0
	rf.VotedFor = -1             // -1 indica "nulo" (nenhum voto concedido)
	rf.Log = make([]LogEntry, 1) // O log começa com índice 1. O índice 0 é um sentinela.

	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.State = StateFollower // Todo servidor começa como Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionLoop() // Inicia o loop de eleição

	return rf
}
