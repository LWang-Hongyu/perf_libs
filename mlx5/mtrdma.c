#define _GNU_SOURCE

#include "mtrdma.h"
#include "mlx5.h"
#include "khash.h"

KHASH_MAP_INIT_INT(qph, uint32_t);
KHASH_MAP_INIT_INT(cqh, uint32_t);

khash_t(qph) * qp_hash;
khash_t(cqh) * cq_hash;

int use_mtrdma = -1;
bool control_stop;

int new_use = 0;

struct ibv_send_wr *send_wr_array[4];
struct ibv_wc *wc_list[150];

static uint32_t new_qp_create = 0;

struct mtrdma_tenant_context tenant_ctx;
struct mtrdma_qp_context *qp_ctx = NULL;
struct mtrdma_cq_context *cq_ctx = NULL;
struct mtrdma_shm_context *shm_ctx = NULL;

static uint32_t tenant_id = -1;
static uint32_t global_qnum = 0;
static uint32_t global_cqnum = 0;
cpu_set_t th_cpu;
cpu_set_t th_cpu_update;
pthread_attr_t th_attr;
pthread_attr_t th_attr_update;
pthread_t daemon_thread;
pthread_t daemon_thread_update;

int run_times = 0;

struct timeval poll_start, poll_end;

int mtrdma_get_sq_num(struct ibv_qp *ibqp)
{
	struct mlx5_qp *qp = to_mqp(ibqp);
	return qp->sq.head - qp->sq.tail;
}

void mtrdma_early_poll_cq()
{
	//LOG_ERROR("perf_early_poll_cq()\n");
	uint32_t cq_poll_num;
	int polled;
	for (uint32_t i = 0; i < global_cqnum; i++) {
		if (cq_ctx[i].cq != NULL) {
			//pthread_mutex_lock(&(cq_ctx[i].lock));
			//LOG_ERROR("before i: %d %d\n",  mlx5_get_sq_num(qp_ctx[0].qp), mlx5_get_sq_num(qp_ctx[1].qp));
			while (1) {
				cq_poll_num =
					cq_ctx[i].max_cqe - cq_ctx[i].wc_tail;
				polled = mlx5_poll_cq_early(
					cq_ctx[i].cq, cq_poll_num,
					(struct ibv_wc *)(cq_ctx[i].wc_list) +
						cq_ctx[i].wc_tail,
					1);

				if (polled < 0) {
					LOG_ERROR("Error in early poll: %d\n",
						  polled);
					exit(1);
				}

				if (!polled)
					break;

				/*
        for(uint32_t j=0; j<polled; j++)
        {
          LOG_ERROR("Comp WR ID: %lu %d\n", ((struct ibv_exp_wc*)cq_ctx[i].wc_list)[cq_ctx[i].wc_tail+j].wr_id, ((struct ibv_exp_wc*)cq_ctx[i].wc_list)[cq_ctx[i].wc_tail+j].status);
        }
        */
				cq_ctx[i].early_poll_num += polled;
				cq_ctx[i].wc_tail =
					(cq_ctx[i].wc_tail + polled) %
					cq_ctx[i].max_cqe;

				//LOG_ERROR("Early Poll CQ: %d, early poll num: %d, wc_tail: %d\n",polled, cq_ctx[i].early_poll_num, cq_ctx[i].wc_tail);
			}
			//LOG_ERROR("after i: %d %d\n",  mlx5_get_sq_num(qp_ctx[0].qp), mlx5_get_sq_num(qp_ctx[1].qp));
			//pthread_mutex_unlock(&(cq_ctx[i].lock));
		}
	}
}

void wr_copy(struct ibv_send_wr *dest_wr, struct ibv_send_wr *src_wr)
{
	struct ibv_sge *origin_sg_list = dest_wr->sg_list;

	memcpy(dest_wr, src_wr, sizeof(struct ibv_send_wr));

	dest_wr->sg_list = origin_sg_list;
	memcpy(dest_wr->sg_list, src_wr->sg_list,
	       sizeof(struct ibv_sge) * src_wr->num_sge);
}

void enqueue_wr(uint32_t q_idx, struct ibv_send_wr *wr)
{
	if (qp_ctx[q_idx].wr_queue_len == qp_ctx[q_idx].wr_queue_size) {
		LOG_ERROR(
			"Error! Waiting SGE Queue is full, cannot enqueue: %d %d %d\n",
			q_idx, qp_ctx[q_idx].wr_queue_len,
			qp_ctx[q_idx].wr_queue_size);
		exit(1);
	}

	wr_copy(&(qp_ctx[q_idx].wr_queue[qp_ctx[q_idx].wr_queue_tail]), wr);
	//LOG_ERROR("Enqueue: %d %d\n", qp_ctx[q_idx].wr_queue[qp_ctx[q_idx].wr_queue_tail].sg_list[0].length, wr->sg_list[0].length);

	uint32_t suc_idx = qp_ctx[q_idx].wr_queue_tail - 1;
	if (qp_ctx[q_idx].wr_queue_tail == 0)
		suc_idx = qp_ctx[q_idx].wr_queue_size - 1;

	if (qp_ctx[q_idx].wr_queue_len > 0 &&
	    qp_ctx[q_idx].wr_queue[suc_idx].next != NULL)
		qp_ctx[q_idx].wr_queue[suc_idx].next =
			&(qp_ctx[q_idx].wr_queue[qp_ctx[q_idx].wr_queue_tail]);

	qp_ctx[q_idx].wr_queue_tail =
		(qp_ctx[q_idx].wr_queue_tail + 1) % qp_ctx[q_idx].wr_queue_size;
	qp_ctx[q_idx].wr_queue_len++;
}

struct ibv_send_wr *get_queued_wr(uint32_t q_idx, uint32_t pwr_idx)
{
	if (qp_ctx[q_idx].wr_queue_len <= pwr_idx) {
		return NULL;
	}

	//LOG_ERROR("%d GET WR: %d, %p\n",q_idx, (qp_ctx[q_idx].wr_queue_head + pwr_idx) % qp_ctx[q_idx].wr_queue_size, &(qp_ctx[q_idx].wr_queue[(qp_ctx[q_idx].wr_queue_head + pwr_idx) % qp_ctx[q_idx].wr_queue_size]));
	return &(
		qp_ctx[q_idx].wr_queue[(qp_ctx[q_idx].wr_queue_head + pwr_idx) %
				       qp_ctx[q_idx].wr_queue_size]);
}

void dequeue_wr(uint32_t q_idx, uint32_t num)
{
	if (qp_ctx[q_idx].wr_queue_len < num) {
		LOG_ERROR(
			"Error! Waiting SGE Queue is empty, cannot dequeue\n");
		exit(1);
	}

	qp_ctx[q_idx].wr_queue_head = (qp_ctx[q_idx].wr_queue_head + num) %
				      qp_ctx[q_idx].wr_queue_size;
	qp_ctx[q_idx].wr_queue_len -= num;

	//LOG_ERROR("%d Dequeue WR: %d\n", q_idx, qp_ctx[q_idx].wr_queue_len);
}

void mtrdma_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr)
{
	uint32_t q_idx = kh_value(qp_hash, kh_get(qph, qp_hash, qp->qp_num));

	if (wr != NULL) {
		enqueue_wr(q_idx, wr);

		while (wr->next != NULL) {
			wr = wr->next;
			enqueue_wr(q_idx, wr);
		}
	}
}

// void mtrdma_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
// 		      struct ibv_send_wr **bad_wr)
// {
// 	if (!new_use) {
// 		for (int i = 0; i < 4; i++) {
// 			send_wr_array[i] = (struct ibv_send_wr *)malloc(
// 				sizeof(struct ibv_send_wr));

// 			memcpy(send_wr_array[i], wr,
// 			       sizeof(struct ibv_send_wr));
// 			memcpy(send_wr_array[i]->sg_list, wr->sg_list,
// 			       sizeof(struct ibv_sge) * wr->num_sge);

// 			send_wr_array[i]->wr_id = -1;
// 			send_wr_array[i]->send_flags = 0;
// 			send_wr_array[i]->sg_list[0].length = 0;
// 		}

// 		for (int i = 0; i < 4; i++) {
// 			if (i < 3) {
// 				send_wr_array[i]->next = send_wr_array[i + 1];
// 			} else {
// 				send_wr_array[i]->next = NULL;
// 			}
// 		}
// 		// wr->next = temp_wr;
// 		wr->next = send_wr_array[0];

// 		for (int i = 0; i < 150; i++) {
// 			wc_list[i] =
// 				(struct ibv_wc *)malloc(sizeof(struct ibv_wc));
// 		}

// 		new_use = 1;
// 	}

// 	// int i = 0;
// 	// struct ibv_send_wr *temp = wr;
// 	// while (temp->next != NULL) {
// 	// 	temp = temp->next;
// 	// 	i++;
// 	// }

// 	// printf("phx number %d\n", i);

// 	int sq_num = mtrdma_get_sq_num(qp);
// 	int max_wr = to_mqp(qp)->sq.max_post;
// 	printf("Max CQE: %d\n", to_mcq(qp->send_cq)->cqe_sz);

// 	if (run_times % 30 == 0) {
// 		// printf("sadasdasdas\n");
// 		wr->send_flags = 0x2;
// 	}

// 	if (sq_num > (max_wr * 3) / 4) {
// 		// LOG_DEBUG("phx test: SQ getting full (%d/%d), polling CQ\n",sq_num, max_wr);
// 		int temp = mtrdma_early_poll_cq(qp->send_cq, 50, wc_list);
// 		//LOG_DEBUG("phx test temp: polled %d completions\n", temp);
// 		if (temp < 0) {
// 			LOG_ERROR(
// 				"phx test: Failed to poll CQ to free up space\n");
// 		}

// 		// Check if we still don't have enough space
// 		// sq_num = mtrdma_get_sq_num(qp);
// 		// if (sq_num > (max_wr * 3) / 4) {
// 		// 	LOG_ERROR(
// 		// 		"phx test: SQ still full after polling, may need to wait\n");
// 		// 	// If we're still too full, we might need to break or wait
// 		// 	if (sq_num > (max_wr * 9) / 10) {
// 		// 		poll_break = 1;
// 		// 		break;
// 		// 	}
// 		// }
// 	}

// 	struct mlx5_qp *mqp = to_mqp(qp);
// 	// printf("The return value of mlx5_get_sq_num is: %d\n",
// 	//        mqp->sq.head - mqp->sq.tail);

// 	run_times++;
// 	gettimeofday(&poll_start, NULL);
// 	mlx5_post_send2(qp, wr, bad_wr);
// 	gettimeofday(&poll_end, NULL);
// 	uint64_t t = (poll_end.tv_sec - poll_start.tv_sec) * 1000000 +
// 		     (poll_end.tv_usec - poll_start.tv_usec);
// 	printf("The time taken is: %lu microseconds\n", t);
// }

void update_mtrdma_state(struct ibv_qp *qp, uint32_t max_send_wr,
			 uint32_t max_recv_wr, uint32_t origin_max_send_wr,
			 uint32_t origin_max_recv_wr, int sig_all)
{
	if (use_mtrdma == -1)
		load_mtrdma_config();

	printf("phx update\n");
	update_qp_ctx(qp, max_send_wr, max_recv_wr, origin_max_send_wr,
		      origin_max_recv_wr, sig_all);
}

void mtrdma_destroy_qp()
{
	if (!use_mtrdma)
		return;

	pthread_mutex_lock(&shm_ctx->lock);
	shm_ctx->active_qps_per_tenant[tenant_id] = 0;
	pthread_mutex_unlock(&(shm_ctx->mtrdma_thread_lock[tenant_id]));
	pthread_mutex_unlock(&shm_ctx->lock);

	pthread_cancel(daemon_thread);

	use_mtrdma = false;
}

void load_mtrdma_config()
{
	use_mtrdma = 1;

	atexit(mtrdma_destroy_qp);

	qp_hash = kh_init(qph);
	cq_hash = kh_init(cqh);

	int shm_fd = shm_open("/mtrdma-shm", O_RDWR, 0);
	if (shm_fd == -1) {
		LOG_ERROR("Cannot load mtrdma_shm\n");
		exit(1);
	}

	shm_ctx = (struct mtrdma_shm_context *)mmap(
		NULL, sizeof(struct mtrdma_shm_context), PROT_READ | PROT_WRITE,
		MAP_SHARED, shm_fd, 0);

	if (shm_ctx == MAP_FAILED) {
		LOG_ERROR("Error mapping shared memory mtrdma_shm");
		exit(1);
	}

	pthread_mutex_lock(&shm_ctx->lock);
	tenant_id = shm_ctx->next_tenant_id;
	tenant_id = shm_ctx->next_tenant_id++;
	shm_ctx->tenant_num++;
	shm_ctx->active_qps_per_tenant[tenant_id] = 0;
	LOG_ERROR("Set Tenant ID: %d\n", tenant_id);
	pthread_mutex_unlock(&shm_ctx->lock);

	CPU_ZERO(&th_cpu);
	CPU_SET(0, &th_cpu);

	CPU_ZERO(&th_cpu_update);
	CPU_SET(10, &th_cpu_update);
	//CPU_SET(1, &th_cpu);
	pthread_attr_init(&th_attr);
	pthread_attr_setaffinity_np(&th_attr, sizeof(th_cpu), &th_cpu);

	pthread_attr_init(&th_attr_update);
	pthread_attr_setaffinity_np(&th_attr_update, sizeof(th_cpu_update),
				    &th_cpu_update);

	sigset_t tSigSetMask;
	sigaddset(&tSigSetMask, SIGALRM);
	pthread_sigmask(SIG_SETMASK, &tSigSetMask, NULL);
}

void mtrdma_thread_end()
{
	sleep(1);

	pthread_mutex_lock(&shm_ctx->lock);
	shm_ctx->active_qps_per_tenant[tenant_id] = 0;
	pthread_mutex_unlock(&(shm_ctx->mtrdma_thread_lock[tenant_id]));
	pthread_mutex_unlock(&shm_ctx->lock);

	exit(1);
}

void mtrdma_update_tenant_state()
{
	struct timeval now;
	gettimeofday(&now, NULL);

	uint64_t t =
		(now.tv_usec - tenant_ctx.last_sq_check_time.tv_usec) +
		1000000 * (now.tv_sec - tenant_ctx.last_sq_check_time.tv_sec);

	uint32_t max = 0;

	if (t >= TENANT_SQ_CHECK_INTERVAL) {
		for (uint32_t i = 0; i < global_qnum; i++) {
			if (max < mtrdma_get_sq_num(qp_ctx[i].qp) +
					  qp_ctx[i].wr_queue_len)
				max = mtrdma_get_sq_num(qp_ctx[i].qp) +
				      qp_ctx[i].wr_queue_len;
		}

		tenant_ctx.sq_history[tenant_ctx.sq_ins_idx] = max;
		if (tenant_ctx.sq_ins_idx == tenant_ctx.sq_max_idx) {
			for (uint32_t i = 0; i < tenant_ctx.sq_history_len;
			     i++) {
				if (tenant_ctx.sq_history[i] >
				    tenant_ctx
					    .sq_history[tenant_ctx.sq_max_idx]) {
					tenant_ctx.sq_max_idx = i;
				}
			}
		}

		tenant_ctx.sq_ins_idx =
			(tenant_ctx.sq_ins_idx + 1) % tenant_ctx.sq_history_len;
		gettimeofday(&(tenant_ctx.last_sq_check_time), NULL);
		//if(tenant_ctx.delay_sensitive)
		//  LOG_ERROR("MAX SQ NUM : %d\n", tenant_ctx.sq_history[tenant_ctx.sq_max_idx]);
	}

	t = (now.tv_usec - tenant_ctx.last_active_check_time.tv_usec) +
	    1000000 * (now.tv_sec - tenant_ctx.last_active_check_time.tv_sec);
}

void *mtrdma_thread(void *para)
{
	signal(SIGKILL, mtrdma_thread_end); // MUST be disabled when using CRAIL
	signal(SIGINT, mtrdma_thread_end);

	while (1) {
		if (new_qp_create)
			pthread_exit(NULL);

		mtrdma_update_tenant_state();

		mtrdma_admittion_control();

		usleep(0);
	}
	return NULL;
}

void *mtrdma_udpate_credit_thread(void *para)
{
	while (1) {
		if (new_qp_create)
			pthread_exit(NULL);

		tenant_ctx.quantam_data += 400000;

		usleep(1);
	}
	return NULL;
}

bool mtrdma_large_process(uint32_t q_idx, struct ibv_send_wr *wr)
{
	struct timeval poll_start, poll_end;
	uint64_t poll_time = 5; //us
	bool poll_break = false;

	if (qp_ctx[q_idx].max_wr - mtrdma_get_sq_num(qp_ctx[q_idx].qp) < 1) {
		gettimeofday(&poll_start, NULL);
		mtrdma_early_poll_cq();
		gettimeofday(&poll_end, NULL);
		uint64_t t = (poll_end.tv_sec - poll_start.tv_sec) * 1000000 +
			     (poll_end.tv_usec - poll_start.tv_usec);
		if (t > poll_time) {
			poll_break = true;
			return false;
		}
		if (qp_ctx[q_idx].max_wr - mtrdma_get_sq_num(qp_ctx[q_idx].qp) <
		    1) {
			return false;
		}
	}

	struct ibv_send_wr **bad_wr;
	if (mlx5_post_send2(qp_ctx[q_idx].qp, wr, bad_wr) != 0) {
		LOG_ERROR("Send dummy error: %d %d\n", qp_ctx[q_idx].max_wr,
			  mtrdma_get_sq_num(qp_ctx[q_idx].qp));
		exit(1);
		return false;
	}

	return true;
}

void mtrdma_admittion_control()
{
	while (1) {
		control_stop = true;
		for (uint32_t i = 0; i < global_qnum; i++) {
			//LOG_ERROR("start perf_wr_queue_manage: %d %d %d\n", i, perf_check_paused(i), qp_ctx[i].wr_queue_len);
			if (!qp_ctx[i].wr_queue_len ||
			    tenant_ctx.quantam_data == 0)
				continue;

			// if (!qp_ctx[i].wr_queue_len)
			// 	continue;

			uint32_t p_num = 0;
			while (qp_ctx[i].wr_queue_len - p_num > 0) {
				//LOG_ERROR("%d %d %d\n", mlx5_get_sq_num(qp_ctx[i].qp), qp_ctx[i].wr_queue_len, p_num);
				struct ibv_send_wr *wr =
					get_queued_wr(i, p_num);

				if (wr == NULL ||
				    wr->sg_list->length >
					    tenant_ctx.quantam_data) {
					// LOG_ERROR("no more credit");
					break;
				}

				// if (wr == NULL) {
				// 	// LOG_ERROR("no more credit");
				// 	break;
				// }

				uint32_t nreq = 1;
				struct ibv_send_wr *tmp = wr;

				while (tmp->next != NULL) {
					if (qp_ctx[i].wr_queue_len == 0) {
						LOG_ERROR(
							"POST BG SEND Error not including singal_wr\n");
						exit(1);
					}
					tmp = get_queued_wr(i, p_num + nreq);
					nreq++;
				}

				if (mtrdma_large_process(i, wr)) {
					// printf("phx test\n");
					p_num++;
					tenant_ctx.quantam_data -=
						wr->sg_list->length;
				} else {
					break;
				}

				if (p_num >= qp_ctx[i].wr_queue_size / 2) {
					dequeue_wr(i, p_num);
					p_num = 0;
				}
			}

			if (p_num) {
				dequeue_wr(i, p_num);
			}
		}

		if (control_stop)
			break;
	}
}

void update_qp_ctx(struct ibv_qp *qp, uint32_t max_send_wr,
		   uint32_t max_recv_wr, uint32_t origin_max_send_wr,
		   uint32_t origin_max_recv_wr, int sig_all)
{
	new_qp_create = 1;
	pthread_join(daemon_thread, NULL);
	pthread_join(daemon_thread_update, NULL);
	new_qp_create = 0;

	int k = kh_get(qph, qp_hash, qp->qp_num);
	if (k != kh_end(qp_hash)) {
		LOG_ERROR("Error! Duplicated QP is created\n");
		exit(1);
	}

	int ret;
	k = kh_put(qph, qp_hash, qp->qp_num, &ret);
	kh_value(qp_hash, k) = global_qnum;

	global_qnum++;
	uint32_t q_idx = global_qnum - 1;

	qp_ctx = (struct mtrdma_qp_context *)realloc(
		qp_ctx, global_qnum * sizeof(struct mtrdma_qp_context));

	qp_ctx[q_idx].qp = qp;
	qp_ctx[q_idx].sig_all = sig_all;
	qp_ctx[q_idx].max_wr = max_send_wr;
	qp_ctx[q_idx].max_recv_wr = max_recv_wr;
	qp_ctx[q_idx].wr_queue_size = origin_max_send_wr * 2 + 10;
	qp_ctx[q_idx].wr_queue = (struct ibv_send_wr *)malloc(
		sizeof(struct ibv_send_wr) * qp_ctx[q_idx].wr_queue_size);

	qp_ctx[q_idx].wr_queue_head = 0;
	qp_ctx[q_idx].wr_queue_tail = 0;
	qp_ctx[q_idx].wr_queue_len = 0;

	for (uint32_t i = 0; i < qp_ctx[q_idx].wr_queue_size; i++) {
		qp_ctx[q_idx].wr_queue[i].sg_list = (struct ibv_sge *)malloc(
			sizeof(struct ibv_sge) * MAX_SGE_LEN);
	}

	update_cq_ctx(qp, origin_max_send_wr);

	if (global_qnum == 1)
		update_tenant_ctx();

	pthread_create(&daemon_thread, &th_attr, mtrdma_thread, NULL);
	pthread_create(&daemon_thread_update, &th_attr_update,
		       mtrdma_udpate_credit_thread, NULL);
}

void update_cq_ctx(struct ibv_qp *qp, uint32_t max_send_wr)
{
	//LOG_ERROR("update_mtrdma_cq_state()\n");
	//updqte cq_ctx
	uint32_t cq_num = 0;
	uint32_t q_idx = global_qnum - 1;
	for (uint32_t i = 0; i < global_qnum - 1; i++) {
		if (qp->send_cq != qp_ctx[i].qp->send_cq)
			cq_num++;
		else {
			cq_num = qp_ctx[i].cq_num;
			cq_ctx[cq_num].max_cqe += max_send_wr * 4;
			cq_ctx[cq_num].wc_list = (void *)realloc(
				cq_ctx[cq_num].wc_list,
				sizeof(struct ibv_wc) *
					(cq_ctx[cq_num].max_cqe + 10));
			break;
		}
	}

	if (global_cqnum == cq_num) {
		cq_ctx = (struct mtrdma_cq_context *)realloc(
			cq_ctx,
			(global_cqnum + 1) * sizeof(struct mtrdma_cq_context));
		cq_ctx[cq_num].max_cqe = max_send_wr * 4;
		cq_ctx[cq_num].wc_list = (void *)malloc(
			sizeof(struct ibv_wc) * (cq_ctx[cq_num].max_cqe + 10));
		cq_ctx[cq_num].cq = qp->send_cq;
		cq_ctx[cq_num].wc_head = 0;
		cq_ctx[cq_num].wc_tail = 0;
		cq_ctx[cq_num].early_poll_num = 0;
		pthread_mutex_init(&(cq_ctx[cq_num].lock), NULL);

		qp_ctx[q_idx].cq_num = cq_num;

		int ret;
		uint32_t k = kh_put(cqh, cq_hash, qp->send_cq->handle, &ret);
		kh_value(cq_hash, k) = cq_num;

		global_cqnum++;
	}
}

void update_tenant_ctx()
{
	//LOG_ERROR("update_mtrdma_tenant_state()\n");

	tenant_ctx.sq_history_len =
		TENANT_SQ_CHECK_WINDOW / TENANT_SQ_CHECK_INTERVAL;
	tenant_ctx.sq_history = (uint32_t *)malloc(tenant_ctx.sq_history_len *
						   sizeof(uint32_t));
	memset(tenant_ctx.sq_history, 0,
	       sizeof(uint32_t) * tenant_ctx.sq_history_len);
	tenant_ctx.sq_ins_idx = 0;
	tenant_ctx.sq_max_idx = 0;

	gettimeofday(&(tenant_ctx.last_sq_check_time), NULL);

	tenant_ctx.avg_msg_size = 0;
	tenant_ctx.max_msg_size = 0;
	tenant_ctx.quantam_data = 0;

	tenant_ctx.active_qps_num = 0;

	gettimeofday(&(tenant_ctx.last_active_check_time), NULL);

	tenant_ctx.additional_enable_num = 0;

	pthread_mutex_init(&(tenant_ctx.poll_lock), NULL);
	pthread_cond_init(&(tenant_ctx.poll_cond), NULL);
}

int mtrdma_poll_cq(struct ibv_cq *cq, uint32_t ne, struct ibv_wc *wc,
		   int cqe_ver)
{
	uint32_t cq_idx = kh_value(cq_hash, kh_get(cqh, cq_hash, cq->handle));

	//pthread_mutex_lock(&(cq_ctx[cq_idx].lock));
	if (cq_ctx[cq_idx].early_poll_num) {
		int ret = cq_ctx[cq_idx].early_poll_num < ne ?
				  cq_ctx[cq_idx].early_poll_num :
				  ne;

		for (uint32_t i = 0; i < ret; i++) {
			memcpy(wc + i,
			       ((struct ibv_wc *)cq_ctx[cq_idx].wc_list) +
				       cq_ctx[cq_idx].wc_head,
			       sizeof(struct ibv_wc));
			// LOG_DEBUG("before real poll: %lu %d\n",
			// 	  (((struct ibv_wc *)cq_ctx[cq_idx].wc_list) +
			// 	   cq_ctx[cq_idx].wc_head)
			// 		  ->wr_id,
			// 	  (((struct ibv_wc *)cq_ctx[cq_idx].wc_list) +
			// 	   cq_ctx[cq_idx].wc_head)
			// 		  ->exp_opcode);
			cq_ctx[cq_idx].wc_head = (cq_ctx[cq_idx].wc_head + 1) %
						 cq_ctx[cq_idx].max_cqe;

			//LOG_ERROR("real poll: %lu %d\n", wc[i].wr_id, wc[i].opcode);
		}

		cq_ctx[cq_idx].early_poll_num -= ret;
		//LOG_ERROR("Copy early polled: %d, wc_head: %d, %d %d\n", ret, cq_ctx[cq_idx].wc_head, cq_idx, cq_ctx[cq_idx].early_poll_num);
		//pthread_mutex_unlock(&(cq_ctx[cq_idx].lock));
		return ret;
	}
	//pthread_mutex_unlock(&(cq_ctx[cq_idx].lock));

	return mlx5_poll_cq_early(cq, ne, wc, cqe_ver);

	/* 
  int ret =   mlx5_poll_cq2(cq, ne, wc, cqe_ver, 1);
  
  if(ret > 0)
  {
    LOG_ERROR("Polled: %d\n", ret);
    for(uint32_t i=0; i<ret; i++)
    {
      LOG_ERROR("WC ID: %ld, Status: %d\n", wc[i].wr_id, wc[i].status);
    }
  }
  return  ret;
  */
}
