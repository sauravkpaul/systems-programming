#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include "packet.h"
#include <time.h>
#include <math.h>

static group_t mgroup_list[MAX_GROUPS];
typedef struct task_thread_
{
    int grp_id;
    task_type_t task_type;
    char file[40];
    unsigned long long low;
    unsigned long long high;
}task_thread_t;

struct timeval tvBegin, tvEnd, tvDiff;

int sorteddelete(struct client_ ** head_ref, int  new_data)
{
    client_t *current, *temp;
#ifdef DEBUG      
    printf("Enter the function sorteddelete \n");
#endif  
    /* Special case for the head end */
    if (*head_ref == NULL || (*head_ref)->sock_fd > new_data)
    {
        printf("Element Not present or empty list \n");
        return -1 ;
    }
    else
    {

        /* Locate the node before the point of insertion */
        current = *head_ref;
        while (current->next!=NULL && current->next->sock_fd < new_data)
        {
            current = current->next;
        }
        if(current->next != NULL && current->next->sock_fd ==  new_data)
        {
            temp = current->next;
            current->next = temp->next;
            free(temp);
            printf("Element found \n");
            return 0 ;
        }
        if(current->sock_fd == new_data)
        {
            //first element is the element to be removed and the only element present 
            printf("Element is removed successfully \n");
            *head_ref = NULL;
            free(current);
            return 0 ;
        }
#ifdef DEBUG
        printf("Element not present in the list \n");
#endif        
        return -1 ;
    }
}

void sortedInsert(struct client_ ** head_ref, int  new_data)
{
    client_t *current;
    /* allocate node */
    client_t *new_node =
        (struct client_*) malloc(sizeof(struct client_));
#ifdef DEBUG
    printf("Enter the function sortedInsert \n");
#endif
    /* put in the data  */
    new_node->sock_fd  = new_data; 
    /* Special case for the head end */
    if (*head_ref == NULL || (*head_ref)->sock_fd >= new_data)
    {
        new_node->next = *head_ref;
        *head_ref = new_node;
    }
    else
    {
        /* Locate the node before the point of insertion */
        current = *head_ref;
        while (current->next!=NULL && current->next->sock_fd < new_node->sock_fd)
        {
            current = current->next;
        }
        new_node->next = current->next;
        current->next = new_node;
    }
}

void mgroup_init( void ) 
{
    int i, j;

    for ( i=0; i<MAX_GROUPS; i++) {
        mgroup_list[i].group_id = i;
        mgroup_list[i].log_file=0;
        mgroup_list[i].no_of_client = 0;
        mgroup_list[i].client_list =   NULL ; 
        client_to_group[i] = -1 ;
        sum[i]= 0 ;
        is_done[i] = -1 ;     
    }
}



static int
make_socket_non_blocking (int sfd)
{
    int flags, s;

    flags = fcntl (sfd, F_GETFL, 0);
    if (flags == -1)
    {
        perror ("fcntl");
        return -1;
    }

    flags |= O_NONBLOCK;
    s = fcntl (sfd, F_SETFL, flags);
    if (s == -1)
    {
        perror ("fcntl");
        return -1;
    }

    return 0;
}

static int
create_and_bind (char *port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int s, sfd;

    memset (&hints, 0, sizeof (struct addrinfo));
    hints.ai_family = AF_UNSPEC;     /* Return IPv4 and IPv6 choices */
    hints.ai_socktype = SOCK_STREAM; /* We want a TCP socket */
    hints.ai_flags = AI_PASSIVE;     /* All interfaces */

    s = getaddrinfo (NULL, port, &hints, &result);
    if (s != 0)
    {
        fprintf (stderr, "getaddrinfo: %s\n", gai_strerror (s));
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next)
    {
        sfd = socket (rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sfd == -1)
            continue;

        s = bind (sfd, rp->ai_addr, rp->ai_addrlen);
        if (s == 0)
        {
            /* We managed to bind successfully! */
            break;
        }

        close (sfd);
    }

    if (rp == NULL)
    {
        fprintf (stderr, "Could not bind\n");
        return -1;
    }

    freeaddrinfo (result);

    return sfd;
}

static void dump_packet_hdr( packet_t *pkt) 
{
#ifdef DEBUG
    //printf("packet id: %s \n", dump_packet_id(pkt->packet_id));
    printf("src client: %d \n", pkt->pkt_client_id);
    printf("group id: %d \n", pkt->pkt_group_id);
#endif
}

static void printf_group_details(group_t grp)
{
    int i = 0 ;
    client_t *temp;
    printf("The details of the Group %d are \n",grp.group_id);
    printf("The Number of clients is/are %d \n",grp.no_of_client);
    printf("The clients are :\n");
    temp = grp.client_list;

    while(temp != NULL){
        printf("Client %d :: %d\n",i,temp->sock_fd);
        temp = temp->next;
        i++;
    }
}    

int gettime()
{
    time_t timer;
    char buffer[25];
    struct tm* tm_info;

    time(&timer);
    tm_info = localtime(&timer);

    strftime(buffer, 25, "%Y:%m:%d %H:%M:%S", tm_info);
    puts(buffer);

    return 0;
}

void * task_thread_api(void* arg)
{
    task_thread_t *task_data = (task_thread_t *)arg;
    int n,grp_id,num_client, divider;
    unsigned long long int k ,sz, low, high;
    FILE *fp;
    char file_name[40];
    client_t *client_hd;
    packet_t *out_pkt;
    grp_id = task_data->grp_id;
    switch(task_data->task_type){
        case  TASK_SUMMATION:
            strcpy(file_name,task_data->file);
            fp=fopen(file_name,"r");
            if (fp == 0)
            {
                fprintf(stderr, "failed to open %s \n", file_name);
                pthread_exit();
            }
#ifdef DEBUG
            printf("file opened \n");
#endif

            gettimeofday(&tvBegin, NULL);
            printf("Begin:%ld.%06ld\n",tvBegin.tv_sec,tvBegin.tv_usec);

            fseek(fp, 0L, SEEK_END);
            sz = ftell(fp);

            //You can then seek back to the beginning:

            fseek(fp, 0L, SEEK_SET);

            fclose(fp);
#ifdef DEBUG
            printf("file closed \n");
#endif

            is_done[grp_id] = 0; 
            num_client=mgroup_list[grp_id].no_of_client;
            recieve_cnt[grp_id] = 0;
            divider = sz/num_client;
            client_hd = mgroup_list[grp_id].client_list;
            k=0;
            while(num_client){
#ifdef DEBUG
                printf("inside while num client %d\n",num_client);
#endif
                out_pkt = (packet_t *) malloc(sizeof(packet_t));
                if(num_client != 1) {
                    out_pkt->start_offset = k;
                    k = k + divider;
                    out_pkt->end_offset = k;


                }else {
                    out_pkt->start_offset = k;
                    out_pkt->end_offset = sz;
                }
                printf("\n");
#ifdef DEBUG 
                printf("PRINT IN TASK THREAD ::Client Number %d, Devider is %d, "
                        "Start offset is %ld, end offset %ld\n",
                        mgroup_list[grp_id].no_of_client-num_client+1, divider,
                        out_pkt->start_offset, out_pkt->end_offset);
#endif                
                out_pkt->packet_id = TASK_EXEC_REQ;
                out_pkt->task_type = TASK_SUMMATION; //XXX
                out_pkt->pkt_group_id= grp_id;
                out_pkt->pkt_client_id = client_hd->sock_fd;
                strcpy(out_pkt->file_name, file_name);

                //send_size = sizeof(packet_t)+ (sizeof(int)*i);

                //send packet 1
                n = send(client_hd->sock_fd, out_pkt, sizeof(packet_t), 0 );
                if(n < 0)
                    perror("send task exec \n");
#ifdef DEBUG
                printf("PRINT IN TASK THREAD ::task sent to client %d sucessfully for grp_id %d \n", 
                        client_hd->sock_fd , grp_id);
#endif        
                num_client--;
                client_hd = client_hd->next;
                //sleep(1);
                //free(temp_data);
                free(out_pkt);
            }
            break;
        case TASK_PRIME_NO: 
            low = task_data->low;
            high= task_data->high;

            gettimeofday(&tvBegin, NULL);
            printf("Begin:%ld.%06ld\n",tvBegin.tv_sec,tvBegin.tv_usec);
            is_done[grp_id] = 0;
            num_client=mgroup_list[grp_id].no_of_client;
            recieve_cnt[grp_id] = 0;
            sz = high-low;
            divider = sz/num_client;
            client_hd = mgroup_list[grp_id].client_list;
            k = low;
            while(num_client) {

                out_pkt = (packet_t *) malloc(sizeof(packet_t));

                if(num_client !=1) {
                    out_pkt->start_offset = k;
                    k = k + divider;
                    out_pkt->end_offset = k;
                }
                else {
                    out_pkt->start_offset = k;
                    out_pkt->end_offset = sz;
                }

                printf("PRINT IN TASK THREAD ::Client Number %d, Devider is %d, "
                        "Start offset is %ld, end offset %ld\n",
                        mgroup_list[grp_id].no_of_client-num_client+1,
                        divider,out_pkt->start_offset,out_pkt->end_offset);
                out_pkt->packet_id = TASK_EXEC_REQ;
                out_pkt->task_type = TASK_PRIME_NO;
                out_pkt->pkt_group_id= grp_id;
                out_pkt->pkt_client_id = client_hd->sock_fd;

                n = send(client_hd->sock_fd, out_pkt, sizeof(packet_t), 0 );

                if(n < 0)
                    perror("send task exec \n");
#ifdef DEBUG
                printf("PRINT IN TASK THREAD ::task sent to client %d sucessfully for grp_id %d \n", 
                        client_hd->sock_fd , grp_id);
#endif
                num_client--;
                client_hd = client_hd->next;
                free(out_pkt);
            }
            break;
        default: break;
    }
}

void * task_cli_handler(void* arg)
{
    int opt, divider, count, grp_id, send_size;
    int i,j,k,l,n,num_client,err;
    char *token, *buf;
    unsigned long long low, high;
    char lines[MAXLINES][BUFSIZE];
    client_t *client_hd;
    char file_name[40];
    task_thread_t data;
    pthread_t tasks_thread;
    while(1)
    {
        printf("Please enter the task type to be done \n");
        printf("1: Addition 2:Prime No 3:Exit\n");
        scanf("%d", &opt);

        switch(opt){
            case 1:
                printf("Enter the file name for arguments: \n");
                scanf("%s",file_name);


                printf("please select a group from below \n");

                for(i=0; i<MAX_GROUPS; i++)
                {
                    if(mgroup_list[i].no_of_client && is_done[i])
                    {
                        printf("Group Number %d has %d clients \n",
                                i,mgroup_list[i].no_of_client);
                    }  
                }
                scanf("%d", &grp_id);
                while(!mgroup_list[grp_id].no_of_client) {
                    printf("Please Enter from the groups given \n");
                    scanf("%d", &grp_id);
                }  
                data.grp_id = grp_id;
                strcpy(data.file,file_name);
                data.task_type = TASK_SUMMATION;
                err = pthread_create(&tasks_thread, NULL, &task_thread_api, 
                        (void *)&data);
                if (err != 0)
                    printf("\ncan't create thread :[%s]\n", strerror(err));

                break;
            case 2:
                printf("Enter the range numbers for finding the primes \n");
                printf("Enter the lower range: \n");
                scanf("%llu", &low);
                printf("Enter the higher range: \n");
                scanf("%llu", &high);

                for(i=0; i<MAX_GROUPS; i++)
                {
                    if(mgroup_list[i].no_of_client && is_done[i])
                    {
                        printf("Group Number %d has %d clients \n",
                                i,mgroup_list[i].no_of_client);
                    }  
                }
                scanf("%d", &grp_id);
                while(!mgroup_list[grp_id].no_of_client) {
                    printf("Please Enter from the groups given \n");
                    scanf("%d", &grp_id);
                }  
                data.grp_id = grp_id;
                data.low = low;
                data.high = high;
                data.task_type = TASK_PRIME_NO;
                err = pthread_create(&tasks_thread, NULL, &task_thread_api, 
                                    (void *)&data);
                if (err != 0)
                    printf("\ncan't create thread :[%s]\n", strerror(err));

                break;
            case 3:
                exit(0);
                break;
            default:
                break;
        }
        sleep(5);
    }
}


int
process_packet_and_respond(int client_fd, packet_t *pkt){
    packet_t out_pkt;
    int grp_id,old_grp_id, n, send_size; 
    int index;
    struct client_ *temp,*temp2;
    FILE *fserver, *fclient;
    char file_name[50];

    switch(pkt->packet_id)
    {
        case JOIN_GROUP_REQ:
            grp_id = pkt->pkt_group_id;

            if( grp_id != 0 )
            {
#ifdef DEBUG                
                printf("recieved join group %d \n", pkt->pkt_group_id);
#endif                

                index = mgroup_list[grp_id].no_of_client;
                sortedInsert( &mgroup_list[grp_id].client_list , client_fd);
                client_to_group[client_fd] = grp_id ;
                mgroup_list[grp_id].no_of_client++;
                printf_group_details(mgroup_list[grp_id]);
                out_pkt.packet_id = JOIN_GROUP_RESP;
                out_pkt.task_type = TASK_NONE;
                out_pkt.pkt_client_id = client_fd;
                out_pkt.pkt_group_id = pkt->pkt_group_id;
                //send the response to client 

                if( send(client_fd, &out_pkt, sizeof(out_pkt), 0 ) <= 0 ) {

                    perror("send response");
                }

            }else {
                // logic to auto join into a group
            }
            break;
        case CHANGE_GROUP_REQ:
            grp_id = pkt->pkt_group_id;

            if( grp_id != 0 )
            {
                printf("recieved Group Change to this  %d and  (old_group id ) "
                        "%d for client %d\n", pkt->pkt_group_id,
                        client_to_group[client_fd],client_fd);

                old_grp_id = client_to_group[client_fd];
                client_to_group[client_fd] = grp_id;
                //let's say it is grp_id_old
                sorteddelete(&mgroup_list[old_grp_id].client_list,client_fd);
                mgroup_list[old_grp_id].no_of_client--;
                printf("Old group details are \n");
                printf_group_details(mgroup_list[old_grp_id]);
                printf("\n");
                index = mgroup_list[grp_id].no_of_client;
                sortedInsert( &mgroup_list[grp_id].client_list , client_fd);
                mgroup_list[grp_id].no_of_client++;
                printf_group_details(mgroup_list[grp_id]);
                out_pkt.packet_id = CHANGE_GROUP_RESP;
                out_pkt.task_type = TASK_NONE;
                out_pkt.pkt_client_id = client_fd;
                out_pkt.pkt_group_id = pkt->pkt_group_id;

                //send the response to client 

                if( send(client_fd, &out_pkt, sizeof(out_pkt), 0 ) <= 0 ) {

                    perror("send response");
                }

            }else {
                // logic to auto join into a group
            }
            break;
        case HEARTBEAT_ALIVE_REQ:
            LOG_MSG(pkt->pkt_group_id,"Heartbeat is received for the client_id "
                    "%d on the group %d",client_fd,pkt->pkt_group_id);
            break;
        case HEARTBEAT_ALIVE_RESP:
        case TASK_EXEC_REQ:
            break;
        case TASK_EXEC_RESP:
            grp_id = pkt->pkt_group_id;
#ifdef DEBUG
            printf("recived a task exec resp for grp %d", grp_id);
#endif
            if(is_done[grp_id] == -1) {
                printf("Error Case \n");
                break ;
            }
            switch(pkt->task_type){
                case TASK_SUMMATION :

                    if(recieve_cnt[grp_id] < mgroup_list[grp_id].no_of_client) 
                    {
                        printf("Received pkt no: %d and sum as %ld\n", 
                                recieve_cnt[grp_id],pkt->payload_data);

                        sum[grp_id] += pkt->payload_data; // acting as a sum result here
                        recieve_cnt[grp_id]++;
                        if(recieve_cnt[grp_id] == mgroup_list[grp_id].no_of_client)
                        {
                            gettimeofday(&tvEnd, NULL);
                            printf("End:%ld.%06ld\n",tvEnd.tv_sec,tvEnd.tv_usec);
                            printf("Final Sum recieved from all the clients for task "
                                    "on group %d is ::  %ld \n",grp_id,sum[grp_id]);

                            struct timeval result;
                            result.tv_sec = ((tvEnd.tv_usec + 1000000 * tvEnd.tv_sec)- 
                                    (tvBegin.tv_usec + 1000000 * tvBegin.tv_sec))/1000000;
                            result.tv_usec = ((tvEnd.tv_usec + 1000000 * tvEnd.tv_sec)- 
                                    (tvBegin.tv_usec + 1000000 * tvBegin.tv_sec))%1000000;
                            printf("Time Taken = %ld.%06ld\n",result.tv_sec, result.tv_usec);

                            is_done[grp_id] = -1;
                            recieve_cnt[grp_id] = 0 ;
                            sum[grp_id] = 0;
                        }
                    }
                    else
                    {
                        printf("More Packets Recieved. Some error there \n");
                    }
                    break;
                case TASK_PRIME_NO:

                    fserver = fopen("solution.txt", "w");
                    if(recieve_cnt[grp_id] < mgroup_list[grp_id].no_of_client)
                    {
                        printf("Received pkt no: %d and file recieved is %s\n", 
                                recieve_cnt[grp_id],pkt->file_name);

                        strcpy(file_name, pkt->file_name);

                        fclient = fopen(file_name , "r");

                        while(!feof(fclient)){
                            putw(getw(fclient), fserver);
                        }

                        recieve_cnt[grp_id]++;
                        fclose(fclient);

                        if(recieve_cnt[grp_id] == mgroup_list[grp_id].no_of_client)
                        {
                            gettimeofday(&tvEnd, NULL);
                            printf("End:%ld.%06ld\n",tvEnd.tv_sec,tvEnd.tv_usec);

                            printf("the result completed and saved in file solution.txt \n");

                            struct timeval result;
                            result.tv_sec = ((tvEnd.tv_usec + 1000000 * tvEnd.tv_sec)- 
                                    (tvBegin.tv_usec + 1000000 * tvBegin.tv_sec))/1000000;
                            result.tv_usec = ((tvEnd.tv_usec + 1000000 * tvEnd.tv_sec)- 
                                    (tvBegin.tv_usec + 1000000 * tvBegin.tv_sec))%1000000;
                            printf("Time Taken = %ld.%06ld\n",result.tv_sec, result.tv_usec);

                            is_done[grp_id] = -1;
                            recieve_cnt[grp_id] = 0 ;
                            sum[grp_id] = 0;
                        }
                    }

                    break;

                default: break;
            }

            break;

        default: break;
    }
    return 0;
}



    int
main (int argc, char *argv[])
{
    int sfd, done,s,grp_id, err;
    int efd;
    struct epoll_event event;
    struct epoll_event *events;
    pthread_t task_cli;
    char *port = "5000";


    sfd = create_and_bind (port);
    if (sfd == -1)
        abort ();

    s = make_socket_non_blocking (sfd);
    if (s == -1)
        abort ();

    s = listen (sfd, SOMAXCONN);
    if (s == -1)
    {
        perror ("listen");
        abort ();
    }

    efd = epoll_create(10);
    if (efd == -1)
    {
        perror ("epoll_create");
        abort ();
    }

    event.data.fd = sfd;
    event.events = EPOLLIN | EPOLLET;
    s = epoll_ctl (efd, EPOLL_CTL_ADD, sfd, &event);
    if (s == -1)
    {
        perror ("epoll_ctl");
        abort ();
    }

    /* Buffer where events are returned */
    events = calloc (MAXEVENTS, sizeof event);

    err = pthread_create(&task_cli, NULL, &task_cli_handler,NULL);
    if (err != 0)
        printf("\ncan't create thread :[%s]\n", strerror(err));

    /* Init all the groups */
    mgroup_init();
    printf("Waiting for connection from client \n");
    /* The event loop */
    while (1)
    {
        int n, i;

        n = epoll_wait (efd, events, MAXEVENTS, -1);
        for (i = 0; i < n; i++)
        {
            if ((events[i].events & EPOLLERR) ||
                    (events[i].events & EPOLLHUP) ||
                    (!(events[i].events & EPOLLIN)))
            {
                /* An error has occured on this fd, or the socket is not
                   ready for reading (why were we notified then?) */
                fprintf (stderr, "epoll error\n");
                close (events[i].data.fd);
                continue;
            }

            else if (sfd == events[i].data.fd)
            {
#ifdef DEBUG
                printf("The request is recieved from the client on server socket [%d]\n",sfd);
#endif
                /* We have a notification on the listening socket, which
                   means one or more incoming connections. */
                while (1)
                {
                    struct sockaddr in_addr;
                    socklen_t in_len;
                    int infd;
                    char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];

                    in_len = sizeof in_addr;
                    infd = accept (sfd, &in_addr, &in_len);
                    if (infd == -1)
                    {
                        if ((errno == EAGAIN) ||
                                (errno == EWOULDBLOCK))
                        {
                            /* We have processed all incoming
                               connections. */
                            break;
                        }
                        else
                        {
                            perror ("accept");
                            break;
                        }
                    }


                    s = getnameinfo (&in_addr, in_len,
                            hbuf, sizeof hbuf,
                            sbuf, sizeof sbuf,
                            NI_NUMERICHOST | NI_NUMERICSERV);
                    if (s == 0)
                    {
                        printf("Accepted connection on descriptor %d "
                                "(host=%s, port=%s)\n", infd, hbuf, sbuf);
                    }

                    /* Make the incoming socket non-blocking and add it to the
                       list of fds to monitor. */
                    s = make_socket_non_blocking (infd);
                    if (s == -1)
                        abort ();

                    event.data.fd = infd;
                    event.events = EPOLLIN | EPOLLET;
                    s = epoll_ctl (efd, EPOLL_CTL_ADD, infd, &event);
                    if (s == -1)
                    {
                        perror ("epoll_ctl");
                        abort ();
                    }
                }
                continue;
            }
            else
            {
                /* We have data on the fd waiting to be read. Read and
                   display it. We must read whatever data is available
                   completely, as we are running in edge-triggered mode
                   and won't get a notification again for the same
                   data. */
                done = 0;

                while (1)
                {
                    ssize_t count;
                    packet_t in_packet;

                    count = read (events[i].data.fd, &in_packet, sizeof(packet_t));
                    if (count == -1)
                    {
                        /* If errno == EAGAIN, that means we have read all
                           data. So go back to the main loop. */
                        if (errno != EAGAIN)
                        {
                            perror ("read");
                            done = 1;
                        }
                        break;
                    }
                    else if (count == 0)
                    {
                        /* End of file. The remote has closed the
                           connection. */
                        done = 1;
                        break;
                    }

                    /* Write the packet buffer to standard output */
#ifdef DEBUG
                    dump_packet_hdr(&in_packet);
#endif

                    //process the incoming packet and respond accordingly

                    process_packet_and_respond(events[i].data.fd, &in_packet);

                }

                if (done)
                {
                    printf ("Closed connection on descriptor %d\n",
                            events[i].data.fd);
                    grp_id = client_to_group[events[i].data.fd];
                    sorteddelete(&mgroup_list[grp_id].client_list,events[i].data.fd);
                    mgroup_list[grp_id].no_of_client--;
                    client_to_group[events[i].data.fd] = -1 ;      


                    /* Closing the descriptor will make epoll remove it
                       from the set of descriptors which are monitored. */
                    close (events[i].data.fd);
                }
            }
        }
    }

    free (events);

    close (sfd);

    return EXIT_SUCCESS;
}
