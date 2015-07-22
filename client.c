#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h> 
#include "packet.h"
#include <pthread.h>
#include <math.h>

typedef struct pthread_data_
{
    int grp_id;
    int sockfd;
}pthread_data_t;

int present_grp_id;
int client_id;
int is_buzy = 0;


static int is_prime(unsigned long long int no)
{
    unsigned long int i;
    int flag = 1;

    if(no == 1)
        return 0;
    
    if(no == 2 || no == 3 || no == 5)
        return 1;

    if(!(no%2) || !(no%3) || !(no%5))
        return 0;

    for(i = 2; i < no/2; i++)
    {
        if(no%i == 0){
            flag = 0;
            break;
        }
    }
    if (flag)
        return 1;
    else
        return 0;
}

int send_recieve_packet(int type, int grp_id, int sockfd)
{
    int n;
    packet_t  *out_pkt,*in_pkt;
    out_pkt = (packet_t *)malloc(sizeof(packet_t));
    in_pkt = (packet_t *)malloc(sizeof(packet_t));

    out_pkt->packet_id=type;
    out_pkt->task_type = TASK_NONE;
    out_pkt->pkt_client_id=0;
    out_pkt->pkt_group_id=grp_id;
    out_pkt->payload_data=0;


    n = send(sockfd,out_pkt,sizeof(packet_t),0);
    if (n < 0)
        error("ERROR sending packet");
#ifdef DEBUG
    printf("send successful\n");
#endif
    n = recv(sockfd,in_pkt,sizeof(packet_t),0);
    if (n < 0) {
        close(sockfd);
        error("ERROR writing to socket");
        free(out_pkt);
        free(in_pkt);    
        return 0;
    }
#ifdef DEBUG    
    printf("recieve successful \n");
#endif
    client_id = in_pkt->pkt_client_id;

    dump_packet_hdr(in_pkt);
    free(in_pkt);
    free(out_pkt);
    return 1 ;
}

void* task_poll_and_execute(void * arg)
{
    pthread_data_t *pthread_data = (pthread_data_t *)arg;
    packet_t *in_pkt ; 
    int sockfd, grp_id, n, send_si, send_size;
    unsigned long long int sum, low, high, i;
    int task_len, *task_data;
    long int sof =0, eof=0, fsize, blk_size; 
    FILE *fp;
    char *buf, *token;
    char file_name[50];
    sockfd = pthread_data->sockfd;
    grp_id = pthread_data->grp_id;

#ifdef DEBUG
    printf("Inside Task thread \n");
#endif

    //conitnuosly poll for the any Task Exec Req
    while(1) {

        sleep(1);
        in_pkt = (packet_t *) malloc(sizeof(packet_t));
        n = recv(sockfd,in_pkt,sizeof(packet_t),0);
        if (n <= 0) {
            continue;
        }
        switch(in_pkt->packet_id)
        {
            case TASK_EXEC_REQ:
                //handle task;
#ifdef DEBUG                
                printf("task execution req recieved for grp %d  \n", in_pkt->pkt_group_id);
#endif                
                switch(in_pkt->task_type)
                {
                    case TASK_SUMMATION: 

#ifdef DEBUG                  
                        printf("file_name recieved: %s start offset:%ld end offset:%ld seek_end %d\n", 
                                in_pkt->file_name, in_pkt->start_offset, in_pkt->end_offset, SEEK_END);
#endif                  
                        fp=fopen(in_pkt->file_name,"r");
                        sof = in_pkt->start_offset;
                        eof = in_pkt->end_offset;
                        is_buzy = 1;
                        fseek(fp, 0L, SEEK_END);
                        fsize=ftell(fp);
                        if(sof != 0){
                            fseek(fp, sof, SEEK_SET);
                            while(fgetc(fp) != ',') 
                                continue;
                            sof = ftell(fp);
                        }
                        if (eof != fsize){
                            fseek(fp,eof,SEEK_SET);
                            while(fgetc(fp)!= ',')
                                continue;
                            fseek(fp, -1L,SEEK_CUR);
                            eof = ftell(fp);
                        }
                        fseek(fp,eof,SEEK_SET);
                        fseek(fp, -1L,SEEK_CUR);
                        eof = ftell(fp);


                        printf("new sof : %ld, new eof: %ld \n", sof, eof);
                        fseek(fp,sof,SEEK_SET);
                        blk_size = eof -sof +1;  
                        buf = (char *) malloc(sizeof(char)*(blk_size+1));
                        if (buf == NULL){
                            printf("Out of Memory \n");
                            exit(1);
                        }
                        fread(buf, 1, blk_size, fp);
                        fclose(fp);
                        buf[blk_size] = ',';
                        buf[blk_size+1] = '\0';
                        token = strtok(buf,", \n");
                        sum = 0 ;
                        while(token) {
#ifdef DEBUG                      
                            printf(": %s :", token);
#endif                      
                            sum +=atoi(token);
#ifdef DEBUG
                            printf("\tpresent sum is %ld \n", sum);
#endif
                            token = strtok(NULL, ",");
                        }
                        free(buf);

                        printf("The sum being sent to Server is: %ld\n", sum);  
                        in_pkt->packet_id=TASK_EXEC_RESP;
                        in_pkt->payload_data=sum;

                        //send_size = sizeof(packet_t)+sizeof(int);

                        n = send(sockfd,in_pkt,sizeof(packet_t),0);
                        if (n<0)
                            perror("ERROR sending packet");
                        is_buzy = 0;       
#ifdef DEBUG                  
                        printf("result sent successfully \n");
#endif                  
                        free(in_pkt);
                        break;

                    case TASK_PRIME_NO:

                        snprintf(file_name, 50, "file_%d_%d", grp_id,in_pkt->pkt_client_id);
#ifdef DEBUG
                        printf("file name: %s \n", file_name);
#endif
                        low = in_pkt->start_offset;
                        high = in_pkt->end_offset;
                        is_buzy =1;
                        fp = fopen(file_name, "w");
                        for(i = low ; i <= high; i++){
                            if(is_prime(i)){
                                //printf("%llu is a prime", i);
                                fprintf(fp, "%llu, ", i);
                            }
                        }

                        printf("\nthe solution is ready on file %s\n", file_name);

                        in_pkt->packet_id=TASK_EXEC_RESP;
                        in_pkt->task_type= TASK_PRIME_NO;
                        strcpy(in_pkt->file_name, file_name);

                        n = send(sockfd,in_pkt,sizeof(packet_t),0);
                        if (n<0)
                            perror("ERROR sending packet");
                        is_buzy = 0;       
                        fclose(fp);
                        free(in_pkt);

                        break;
                }
                break;
            default:
                //printf( "unknown packet recieved %d \n", in_pkt->packet_id);
                break;
        }
    }
}


void* group_change(void* arg)
{
    pthread_data_t *pthread_data = (pthread_data_t *)arg;
    int sockfd = pthread_data->sockfd ,grp_id,n,grp_id_new,ret;
    int c ;
    packet_t *out_pkt ; 
    grp_id = present_grp_id ;
    printf("1) change group \n2) exit \n");
    while(1){
        scanf("%d", &c);
        switch(c) {
            case 1: 
                if(is_buzy){
                    printf("Currently executing a task. Please hold on for some time , Thanks \n");
                    continue;
                }
                printf("Enter the New group ID \n");
                scanf("%d",&grp_id_new);
                while(grp_id_new == grp_id)
                {
                    printf("You have entered same group ID. Please give a different ID \n");
                    scanf("%d",&grp_id_new);
                }
                present_grp_id = grp_id_new ; 
                ret = send_recieve_packet(CHANGE_GROUP_REQ,grp_id_new,sockfd);
                
                printf("group changed succesfully \n");
                break;
            case 2: 
                printf("exiting \n");
                exit(0);
                break;
            default: continue;
        }
    }
}
void* periodic_heartbeat(void* arg) 
{
    packet_t  *out_pkt;
    pthread_data_t *pthread_data = (pthread_data_t *)arg;
    int sockfd = pthread_data->sockfd ,n;

    while(1)
    {
        sleep(30);
        out_pkt = (packet_t *)malloc(sizeof(packet_t));

        out_pkt->packet_id=HEARTBEAT_ALIVE_REQ;
        out_pkt->task_type = TASK_NONE;
        out_pkt->pkt_client_id=client_id;
        out_pkt->pkt_group_id=present_grp_id;

        n = send(sockfd,out_pkt,sizeof(packet_t),0);
        if (n < 0)
            error("ERROR sending packet");

        free(out_pkt);
    }
    return NULL;

}


static void dump_packet_hdr( packet_t *pkt) {
    //printf("packet id: %s \n", dump_packet_id(pkt->packet_id));
#ifdef DEBUG    
    printf("src client: %d \n", pkt->pkt_client_id);
    printf("group id: %d \n", pkt->pkt_group_id);
#endif    
}

int main(int argc, char *argv[])
{
    int sockfd = 0, n = 0, grp_id=0,err;
    struct sockaddr_in serv_addr;
    pthread_data_t pthread_data;
    packet_t *out_pkt, *in_pkt; 
    pthread_t node_alive, cli_thread, task_thread;
    if(argc != 3)
    {
        printf("\n Usage: %s <ip of server> <group_no> \n",argv[0]);
        return 1;
    } 

    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Error : Could not create socket \n");
        return 1;
    } 

    grp_id = atoi(argv[2]);
    present_grp_id =  grp_id;
    memset(&serv_addr, '0', sizeof(serv_addr)); 

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT_NO); 

    if(inet_pton(AF_INET, argv[1], &serv_addr.sin_addr)<=0)
    {
        printf("\n inet_pton error occured\n");
        return 1;
    } 

    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\n Error : Connect Failed \n");
        return 1;
    } 

    n = send_recieve_packet(JOIN_GROUP_REQ,grp_id,sockfd);
    pthread_data.grp_id = grp_id;
    pthread_data.sockfd = sockfd;
    err = pthread_create(&node_alive, NULL, &periodic_heartbeat,(void*)&pthread_data);
    if (err != 0)
        printf("\ncan't create thread :[%s]\n", strerror(err));
    err = pthread_create(&cli_thread, NULL, &group_change,(void*)&pthread_data);
    if (err != 0)
        printf("\ncan't create thread :[%s]\n", strerror(err));
    err = pthread_create(&task_thread, NULL, &task_poll_and_execute, (void *)&pthread_data);
    if (err != 0)
        printf("\ncan't create thread :[%s]\n", strerror(err));
    pthread_join(node_alive, NULL);   
    pthread_join(cli_thread, NULL);
    pthread_join(task_thread, NULL);
    return 0;;
}
