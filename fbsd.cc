#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include "fb.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using assignment2::Post;
using assignment2::ShowList;
using assignment2::ClientRequest;
using assignment2::ServerReply;
using assignment2::FBChatServer;
using assignment2::RegisterServer;
using assignment2::DataSync;

class ServerConnect;

struct Client{
  std::string username;
  bool connect_status = false;
  std::vector<Client*> joined;
  ServerReaderWriter<Post, Post>* stream = 0;
};

struct primaryWorkerInfo{
    std::string hostname;
    std::string port;
    int connected_clients;
}p_worker_info[3] = {{"localhost","6005",0},{"localhost","6008",0},{"localhost","6004",0}};

std::vector<Client> client_db; // read from local file when we start server
bool isMaster = false;
bool isLeader = false;
std::string localPort="10000";
bool isServerConnector=false;
std::string localHostName="localhost";

ServerConnect* masterPrimaryWorker;
ServerConnect* server1PrimaryWorker;
ServerConnect* server2PrimaryWorker;

std::string masterServerAddr="localhost";
std::string masterConnectorPort="6004";
//string ServerHostAddr[3];
int serverID = 0;
int slaveServerStatus[3];	//0-master,1-server1,2-server2

//return the index of target client
//if not find ,return -1 
int find_user(std::string username){
    int index = 0;
    for(Client c: client_db){
        if(c.username == username) return index;
        index++;
    }
    return -1;
}

//Each time when client "JOIN", "LEAVE" or log in, we should synchronize
//our client database
void UpdateDatabase(Client *client){
    
    //easy implement for serializing client(use its username and the clients it has joined)
    std::string update = client->username + " joined:";
    for(Client *c: client->joined){
        update += "~" + c->username;
    }
    
    //find the old data for the client, delete it and add new data to the tail of the file
    std::string filename = localHostName + "client_database.txt";
    std::fstream file(filename);
    std::string line;
    std::ofstream outfile("in2.txt",std::ios::out|std::ios::trunc);
    while(!file.eof())
    {
         if(std::getline(file,line)){
         int space = line.find(" ");
         std::string name = line.substr(0, space); 
         if(name == client->username) continue;
           outfile << line << '\n';
       }
    }
    outfile.close();
    file.close();
    
    std::ofstream outfile1(filename,std::ios::out|std::ios::trunc);
    std::fstream file1("in2.txt");
    while(!file1.eof())
    {
         if(std::getline(file1,line))
         outfile1 << line << '\n';
    }
    outfile1<<update<< '\n';
    outfile1.close();
    file1.close();
    remove("in2.txt");//Delete temp txt
    return;
}

//When server start, we read from client database
//deserialize each data and store it to memory(client_databae)
void LoadDatabase(){
    client_db.clear();
    std::string filename = localHostName + "client_database.txt";
    std::fstream file1(filename);
    std::string line;
    
    //we need to read database 2 times
    //first time to get all users' username
    //second time to get their joined relationship
    
    //1st
    while(!file1.eof())
    {
         if(std::getline(file1,line)){
             Client c;
             int space = line.find(" ");
             c.username = line.substr(0, space);
             client_db.push_back(c);
         }
    }
    file1.close();
    
    //2nd
    std::fstream file2(filename);
    while(!file2.eof())
    {
         if(std::getline(file2,line)){
            int space = line.find(" ");
            std::string name = line.substr(0, space);
            Client *cur = &client_db[find_user(name)];
            // add clients that this client has joined to the vector
            std::size_t index = line.find(":");
            if(index == -1) continue; // this client has not joined any other clients
            else{
                //parse the data
                //deserialize
                std::string joined = line.substr(index + 1);
                int l = 0;
                for(int i = 0; i < joined.size(); i++){
                    if(joined[i] == '~'){
                        if(i >= l + 2){
                            std::string n = joined.substr(l + 1, i - l -1);
                            Client *user = &client_db[find_user(n)];
                            cur->joined.push_back(user);
                            l = i;
                        }
                    }
                }
                if(joined.size() > 0){
                std::string n = joined.substr(l + 1);
                int i = find_user(n); 
                Client *user = &client_db[i];
                cur->joined.push_back(user);
                }
            }
         }
    }
    file2.close();
    return;
}

//mainly use this function at the beginging to check whether the file existed
//if not , we should create a blank one.
bool CheckFile(std::string name){
    std::ifstream file(name);
    return file.good();
}

//Once client make a post, we write it into file
//and we should write the newest post in the head of the file
void ModifyTimeLine(std::string msg, std::string username){
    
    std::string filename = localHostName + username + ".txt";
    
    std::fstream file(filename);
    std::string line;
    std::ofstream outfile("in2.txt",std::ios::out|std::ios::trunc);
    outfile << msg << '\n';
    while(!file.eof())
    {
         if(std::getline(file,line)){
           outfile << line << '\n';
       }
    }
    outfile.close();
    file.close();
    //std::string temp = username + ".txt";
    //const char *k = temp.c_str();
    const char *k = filename.c_str();
    rename(k, "del.txt");
    rename("in2.txt", k);
    remove("del.txt");//Delete temp txt
    return;
}
 
//Class used for slave server to connect to master server
class ServerConnect{
public:
	ServerConnect(std::shared_ptr<Channel> channel, std::string server_Name){
		serverStub = RegisterServer::NewStub(channel);
		serverName = server_Name;
	}

	std::string ServerRegister(){
		ClientContext context;
		ServerReply slaveServerRequest;
		ServerReply masterServerReply;
		slaveServerRequest.set_message(serverName);
		slaveServerRequest.set_id(serverID);
		std::cout<<"Hello World! from slave server"<<std::endl;
		Status status = serverStub->ServerRegister(&context, slaveServerRequest, &masterServerReply);
		if(status.ok()) return masterServerReply.message();

		std::cout<< "failed at Slave Server Register\n";
		abort();
	}
    
    void updateDatabase(std::string update, std::string username){
		ClientContext context;
		DataSync primaryUpdateRequest;
		ServerReply PrimaryUpdateReply;
		primaryUpdateRequest.set_message(update);
		primaryUpdateRequest.set_username(username);
        primaryUpdateRequest.set_servername(localHostName);
		std::cout<<"Synchronizing with other workers"<<std::endl;
        
		Status status = serverStub->updateDatabase(&context, primaryUpdateRequest, &PrimaryUpdateReply);
        
        std::cout<<"Finished"<<std::endl;
		if(status.ok()) return;

		std::cout<< "failed at Slave Server Register\n";
		abort();
    }

private:
	std::string serverName;
	std::unique_ptr<RegisterServer::Stub> serverStub;
};

class ServerConnectImpl final:public RegisterServer::Service{

	Status ServerRegister(ServerContext* context, const ServerReply* request, ServerReply* reply) override {

	std::cout<<"Hello World! from master server"<<std::endl;
	int request_server_id = (int)(request->id());
	slaveServerStatus[request_server_id] = 1;
//	std::string requestServerName = ;
//	std::cout<<"Hello World! from master server\n"<<std::endl;
	std::string register_reply = request->message()+" Register Successful!";
	
      reply->set_message(register_reply);
	reply->set_id(0);

    return Status::OK;
  }
  
	Status updateDatabase(ServerContext* context, const DataSync* request, ServerReply* reply) override {
	std::cout<<"upating  database"<<std::endl;
    
    std::string update = request->message();
    std::string username = request->username();
    
    std::string filename = localHostName + "client_database.txt";
    
    std::fstream file(filename);
    std::string line;
    std::ofstream outfile("in2.txt",std::ios::out|std::ios::trunc);
    while(!file.eof())
    {
         if(std::getline(file,line)){
         int space = line.find(" ");
         std::string name = line.substr(0, space); 
         if(name == request->username()) continue;
           outfile << line << '\n';
       }
    }
    outfile.close();
    file.close();
    
    std::ofstream outfile1(filename,std::ios::out|std::ios::trunc);
    std::fstream file1("in2.txt");
    while(!file1.eof())
    {
         if(std::getline(file1,line))
         outfile1 << line << '\n';
    }
    outfile1<<update<< '\n';
    outfile1.close();
    file1.close();
    remove("in2.txt");//Delete temp txt
    
    //update to memory
    LoadDatabase();
    if(isMaster == true && isServerConnector==true){
        if(request->servername() == "server1"){
            server2PrimaryWorker->updateDatabase(update, username);
        }
        if(request->servername() == "server2"){
            server1PrimaryWorker->updateDatabase(update, username);
        }
        
    }

    return Status::OK;
  }


};


//The stub functions let client call
//Communication with client
class FBChatServerImpl final : public FBChatServer::Service {
    
  Status Connect(ServerContext* context, const ClientRequest* request, ServerReply* reply) override {
        std::cout<< "Master side"<<std::endl;
        std::string primaryWorkerAddress;
        if(p_worker_info[0].connected_clients <= p_worker_info[1].connected_clients){
            primaryWorkerAddress = p_worker_info[0].hostname + ":" + p_worker_info[0].port;
            p_worker_info[0].connected_clients++;
            
        }
        else{
            primaryWorkerAddress = p_worker_info[1].hostname + ":" + p_worker_info[1].port;
            p_worker_info[1].connected_clients++;
        }
      reply->set_message(primaryWorkerAddress);
      return Status::OK;
    }
    
  //Sends the list of total rooms and joined rooms to the client
  Status List(ServerContext* context, const ClientRequest* request, ShowList* showlist) override {
    Client user = client_db[find_user(request->username())];
    for(Client c : client_db){
      showlist->add_all_clients(c.username);
    }
    for(Client* c: user.joined){
        showlist->add_joined_clients(c->username);
    }
    return Status::OK;
  }

  //Set user1 join user2 so that user2 can see what user1 had posted
  //modify database
  Status Join(ServerContext* context, const ClientRequest* request, ServerReply* reply) override {
    std::string username1 = request->username();
    std::string username2 = request->requestinfo(0);
    int join = find_user(username2);
    //If you try to join a non-existent client or yourself, send failure message
    if(join == -1|| username1 == username2)
      reply->set_message("Join Failed -- Username Not Exist or Own Username");
    else{
        Client *user2 = &client_db[join];
        Client *user1 = &client_db[find_user(username1)];
      //If user1 already join user2, send failure message
        for(Client* c: user1->joined){
            if(c->username == user2->username){
                    reply->set_message("Join Failed -- Alredy Joined This User");
                    return Status::OK;
            }
        }
      user1->joined.push_back(user2);
      
      UpdateDatabase(user1);
      
      std::string update = username1 + " joined:";
      for(Client *c: user1->joined){
          update += "~" + c->username;
      }
      
      masterPrimaryWorker->updateDatabase(update, user1->username);
      
      
      reply->set_message("Join Successful");
    }
    return Status::OK; 
  }

  //Sets user1 leave user2, user2 can not see user1's posts
  //modify database
  Status Leave(ServerContext* context, const ClientRequest* request, ServerReply* reply) override {
    std::string username1 = request->username();
    std::string username2 = request->requestinfo(0);
    int leave = find_user(username2);
    //If you try to leave a non-existent, send failure message
    //Also, you can not leave yourself
    if(leave == -1 || username1 == username2)
      reply->set_message("Leave Failed -- Username Not Exist or Own Username");
    else{
      Client *user1 = &client_db[find_user(username1)];
      Client *user2 = &client_db[leave];
      int count = 0;
      //If user1 hasn't joined user2, send failure message
      for(Client* c: user1->joined){
          if(c->username == user2->username){
                  user1->joined.erase(user1->joined.begin() + count); 
                  UpdateDatabase(user1);
                  
                  std::string update = username1 + " joined:";
                  for(Client *c: user1->joined){
                      update += "~" + c->username;
                  }
                  
                  masterPrimaryWorker->updateDatabase(update, username1);
                  reply->set_message("Leave Successful");
                  return Status::OK;
          }
          count++;
      }
    }
    reply->set_message("Leave Failed -- Not Joined Yet");
    return Status::OK;
  }

  //Called when the client startd and checks whether their username is taken or not
  Status Login(ServerContext* context, const ClientRequest* request, ServerReply* reply) override {
      std::cout<< "Primary Worker side"<<std::endl;
    Client c;
    std::string username = request->username();
    std::string filename = localHostName + username +".txt";
    int idx = find_user(username);
    if(idx == -1){  // first timelogin
      c.username = username;
      c.connect_status = true;
      client_db.push_back(c);
      if(!CheckFile(filename)){
          std::ofstream fout(filename,std::ios::out);
          fout.close();
      }
      reply->set_message("Login Successful!");
      UpdateDatabase(&c);
      
      std::string update = c.username + " joined:";
      for(Client *c: c.joined){
          update += "~" + c->username;
      }
      masterPrimaryWorker->updateDatabase(update, c.username);
    }
    else{ 
      Client *user = &client_db[idx];
      if(user->connect_status){
        reply->set_message("Invalid Username");
        }
      else{
          if(!CheckFile(filename)){
              std::ofstream fout(filename,std::ios::out);
              fout.close();
          }
        std::string msg = "Welcome Back " + user->username;
        reply->set_message(msg);
        user->connect_status = true;
      }
    }
    return Status::OK;
  }


  //let client makes post and forwards it to all the clients it has joined
  //use ServerReaderWriter to communicate with client
  Status Chat(ServerContext* context, 
    ServerReaderWriter<Post, Post>* stream) override {
    Post post;
    Client *c;
    bool first = true;
    //Read messages until the client shutdown
    while(stream->Read(&post)) {
      std::string username = post.username();
      int user_index = find_user(username);
      c = &client_db[user_index];
      
      std::cout<< post.content() << std::endl;
      
      std::string filename = localHostName + username +".txt";
      
      google::protobuf::Timestamp tp = post.timestamp();
      std::string time = google::protobuf::util::TimeUtil::ToString(tp);
      std::string fileinput = username + " :: " + time + " :: " +post.content();

      //first connection------(After client enter chat mode)
      //show the client last 20 posts
      if(post.content() == "20" && first == true){
            if(c->stream == 0)
      	        c->stream = stream;
            //no record exist, create one
            if(!CheckFile(filename)){
                std::ofstream fout(filename,std::ios::out|std::ios::app);
                fout.close();
            }
            std::ifstream history(filename);
            std::string line;
            int count = 0;
            Post prev;
            prev.set_username("TimeLine");
            while(!history.eof())
            {
                 if(std::getline(history,line)){
                     prev.set_content(line);
                     stream->Write(prev);
                     if(++count == 20) break;
               }
            }
            first = false;
      } 
      else{  
        //put the meassge to the sender's timeline history
          ModifyTimeLine(fileinput,username);
          
      //Send messages to all clients this client has been joined by using ServerReaderWriter
        for(Client* temp_client: c->joined){
            if(temp_client->stream!=0 && temp_client->connect_status)
                temp_client->stream->Write(post); 

            //write to client's timeline history
            std::string temp_username = temp_client->username;
            ModifyTimeLine(fileinput,temp_username);
            
            }
        }
    }
    //If the client disconnected from Chat Mode, set connected to false
    c->connect_status = false;
    c->stream = 0;
    return Status::OK;
  }
};

void connectSetup(){
    

	if(isMaster==false && isServerConnector==true){
		std::string server_register_connection = masterServerAddr+":"+masterConnectorPort;
		std::shared_ptr<Channel> server_register_channel = grpc::CreateChannel(server_register_connection,grpc::InsecureChannelCredentials());
		ServerConnect *server_connect = new ServerConnect(server_register_channel, localHostName);
		std::string serverRegisterReply = server_connect->ServerRegister();
		std::cout << serverRegisterReply << std::endl;
	}

	if(isMaster==true && isServerConnector==true){
        
		std::string s1Primary = "localhost:6005";
		std::shared_ptr<Channel> primary_channel_1 = grpc::CreateChannel(s1Primary,grpc::InsecureChannelCredentials());
		server1PrimaryWorker = new ServerConnect(primary_channel_1, localHostName);
        
		std::string s2Primary = "localhost:6008";
		std::shared_ptr<Channel> primary_channel_2 = grpc::CreateChannel(s2Primary,grpc::InsecureChannelCredentials());
		server2PrimaryWorker = new ServerConnect(primary_channel_2, localHostName);
        
		std::string server_address = "localhost:"+localPort;
		ServerConnectImpl serverConnectService;
		ServerBuilder builder;
		builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		builder.RegisterService(&serverConnectService);
		std::unique_ptr<Server> server(builder.BuildAndStart());
		std::cout << "master server connector is listening on "<<server_address<<std::endl;
		server->Wait();
		
	}
    
    //PrimaryWorker create channel to communicate with other PrimaryWorkers to syschronize data
    //for process not on master server
    if(isMaster == false && isLeader == true){
		std::string masterPrimary = masterServerAddr + ":" + masterConnectorPort;
		std::shared_ptr<Channel> primary_channel = grpc::CreateChannel(masterPrimary,grpc::InsecureChannelCredentials());
		masterPrimaryWorker = new ServerConnect(primary_channel, localHostName);
        
		std::string server_address = "localhost:"+localPort;
		ServerConnectImpl serverConnectService;
        FBChatServerImpl service;
        
		ServerBuilder builder;
		builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		builder.RegisterService(&serverConnectService);
        builder.RegisterService(&service);
		std::unique_ptr<Server> server(builder.BuildAndStart());
		std::cout << "Primary Worker connector is listening on "<<server_address<<std::endl;
		server->Wait();
    }
    return ;
}

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:" + port_no;
  FBChatServerImpl service;

  ServerBuilder builder;
  std::cout << "t1"<< std::endl;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  std::cout << "t1"<< std::endl;
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::cout << "t1"<< std::endl;
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening for clients on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {

    // Parses options that start with '-' and adding ':' makes it mandontory
      int opt = 0;
	
	while((opt=getopt(argc, argv, "p:l:m:c:h:s:i:")) != -1){
		switch(opt){
			case 'p':localPort=optarg;		break;		
			case 'l':isLeader=atoi(optarg);		break;
			case 'm':isMaster=atoi(optarg);		break;
			case 'h':localHostName=optarg;		break;
			case 's':masterServerAddr=optarg;	break;
			case 'i':serverID=atoi(optarg);		break;
			case 'c':isServerConnector=atoi(optarg);		break;
			default: std::cerr << "Please enter valid command\n";
		}
	}	
//      std::string port;
//      port = argv[1];
	if(isMaster == true && isLeader == true){
		slaveServerStatus[0] = 1;	
		slaveServerStatus[1] = 0;
		slaveServerStatus[2] = 0;
	}
    
    
	if(!localPort.compare("6001"))	printf("6001 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6002"))	printf("6002 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6003"))	printf("6003 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6004"))	printf("6004 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6005"))	printf("6005 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6006"))	printf("6006 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6007"))	printf("6007 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6008"))	printf("6008 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6009"))	printf("6009 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
	if(!localPort.compare("6010"))	printf("6010 l m c id hs: %d, %d, %d, %d, %s\n",isLeader, isMaster, isServerConnector, serverID, localHostName.c_str());
    
    /*pthread_t thread_id;
    sleep(1);
    pthread_create(&thread_id, NULL, connectSetup, NULL);
    sleep(1);*/

  //cheack if database file is existed or not
    std::string filename = localHostName + "client_database.txt";
         if(!CheckFile(filename)){
          std::ofstream fout(filename,std::ios::out);
          fout.close();
      }
      
  std::cout << "Loading Database" << std::endl;
  LoadDatabase();
  std::cout << "Loading Successful" << std::endl;
  
  connectSetup();
  RunServer(localPort);

  return 0;
}
