#ifndef MTRDMA_H
#define MTRDMA_H

#include <infiniband/verbs.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sched.h>
#include <unistd.h>
#include <stdatomic.h>
#include <arpa/inet.h>

#define LOG_LEVEL 3

#define COLOR_BLUE "\033[94m"
#define COLOR_GREEN "\033[32m"
#define COLOR_RED "\033[31m"
#define COLOR_RESET "\033[0m"

#if ((LOG_LEVEL) >= 3)
#define LOG_DEBUG(s, a...) printf(COLOR_BLUE "[DEBUG] " s COLOR_RESET, ##a)
#else
#define LOG_DEBUG(s, a...)
#endif

#if ((LOG_LEVEL) >= 2)
#define LOG_INFO(s, a...) printf(COLOR_GREEN "[INFO] " s COLOR_RESET, ##a)
#else
#define LOG_INFO(s, a...)
#endif

#if ((LOG_LEVEL) >= 1)
#define LOG_ERROR(s, a...) printf(COLOR_RED "[ERROR] " s COLOR_RESET, ##a)
#else
#define LOG_ERROR(s, a...)
#endif

#define CHUNK_SIZE 1024

#define MAX_TENANT_NUM 3000
#define MAX_SGE_LEN 16

#define TENANT_SQ_CHECK_INTERVAL 5000 //us
#define TENANT_SQ_CHECK_WINDOW 1000000 //us

#define MTRDMA_LARGE_WR 4096

// mtrdma global functions
int mtrdma_get_sq_num(struct ibv_qp *ibqp);
void mtrdma_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr);
void update_mtrdma_state(struct ibv_qp *qp, uint32_t max_send_wr,
			 uint32_t max_recv_wr, uint32_t origin_max_send_wr,
			 uint32_t origin_max_recv_wr, int sig_all);
int mtrdma_poll_cq(struct ibv_cq *cq, uint32_t ne, struct ibv_wc *wc,
		   int cqe_ver);

struct mtrdma_tenant_context {
	uint32_t sq_history_len;
	uint32_t *sq_history;
	uint32_t sq_ins_idx;
	uint32_t sq_max_idx;

	struct timeval last_sq_check_time;

	double avg_msg_size;
	uint64_t max_msg_size;

	struct timeval last_active_check_time;

	uint32_t active_qps_num;
	pthread_mutex_t active_lock;

	uint32_t additional_enable_num;
	uint64_t quantam_data;

	pthread_mutex_t poll_lock;
	pthread_cond_t poll_cond;
};

struct mtrdma_qp_context {
	struct ibv_qp *qp;

	int sig_all;
	uint32_t max_wr;
	uint32_t max_recv_wr;

	uint32_t cq_num;

	struct ibv_send_wr *wr_queue;
	uint32_t wr_queue_head;
	uint32_t wr_queue_tail;
	atomic_int wr_queue_len;
	uint32_t wr_queue_size;

	struct timeval last_allowed_time;

	uint32_t post_num;

	uint64_t chunk_sent_bytes;
};

struct mtrdma_cq_context {
	struct ibv_cq *cq;
	uint32_t max_cqe;
	atomic_int early_poll_num;

	void *wc_list;
	uint32_t wc_head;
	uint32_t wc_tail;

	pthread_mutex_t lock;
};

struct mtrdma_shm_context {
	uint32_t next_tenant_id;
	uint32_t tenant_num;
	uint32_t active_tenant_num;
	uint64_t active_qps_num;
	uint32_t max_qps_limit;

	uint32_t active_qps_per_tenant[MAX_TENANT_NUM];

	pthread_mutex_t mtrdma_thread_lock[MAX_TENANT_NUM];
	pthread_cond_t mtrdma_thread_cond[MAX_TENANT_NUM];
	pthread_mutex_t lock;
};

#endif