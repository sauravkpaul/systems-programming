
#define MAXEVENTS 64
#define MAX_CLIENTS 256
#define MAX_GROUPS  1000

#define HEARTBEAT_INTERVAL 3000
#define HEARTBEAT_TIMEOUT 10000

enum { MAXLINES = 5 };
#define BUFSIZE 256

#define PORT_NO 5000

#define MAX_MSG 256
#define LOG_MSG(group,format, args...) \
    do {\
        char str1[MAX_MSG];\
        char buff[20];\
        char fn[20];\
        FILE *fp;\
        struct tm *sTm;\
        snprintf(fn,20,"group_%d",group);\
        fp = fopen(fn,"a+");\
        time_t now = time (0);\
        sTm = gmtime(&now);\
        if(NULL != fp)\
        {\
            strftime (buff, sizeof(buff), "%Y-%m-%d %H:%M:%S", sTm);\
            snprintf(str1,MAX_MSG,format,##args);\
            fprintf(fp,"%s:%s\n",buff,str1);\
            fflush(fp);\
            fclose(fp);\
        }\
    }while(0)

//#define DEBUG

unsigned long long int sum[MAX_GROUPS];
int is_done[MAX_GROUPS];
int recieve_cnt[MAX_GROUPS];

typedef struct client_ {
    int sock_fd;
    int64_t expiry;
    struct  client_ *next ;    
} client_t;

int client_to_group[1000];

typedef struct group_ {
    int16_t group_id;
    int log_file;
    int16_t no_of_client;
    client_t *client_list;
} group_t;


typedef enum packet_id_ {
    JOIN_GROUP_REQ,
    JOIN_GROUP_RESP,
    CHANGE_GROUP_REQ,
    CHANGE_GROUP_RESP,
    HEARTBEAT_ALIVE_REQ,
    HEARTBEAT_ALIVE_RESP,
    TASK_EXEC_REQ,
    TASK_EXEC_RESP,
} packet_id_t;

typedef enum task_type_{
    TASK_SUMMATION,
    TASK_PRIME_NO,
    TASK_NONE,
}task_type_t;

typedef enum payload_type_{
    PAYLOAD_FILE,
    PAYLOAD_INT,
    PAYLOAD_NONE,
}payload_type_t;

typedef struct packet_ {
    packet_id_t packet_id;
    task_type_t task_type;
    int16_t     pkt_client_id;
    int16_t     pkt_group_id;
    payload_type_t payload_type;
    unsigned long long int    payload_data;
    long int         start_offset;
    long int         end_offset;
    char            file_name[50];
}packet_t;

static int make_socket_non_blocking (int sfd);
static int create_and_bind (char *port);
static void dump_packet_hdr( packet_t *pkt);
int process_packet_and_respond(int client_fd, packet_t *pkt);


