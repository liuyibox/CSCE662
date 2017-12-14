#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
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


std::vector<Client> client_db; // read from local file when we start server
std::vector<std::string> connected_clients; // record the username of clients that connect to the server

bool isMaster = false;
bool isLeader = false;
std::string localPort="10000";
std::string localServer="lenss-comp1.cse.tamu.edu:";
std::string leaderAddr="lenss-comp1.cse.tamu.edu:9000";
bool isServerConnector=false;

std::string localHostName="localhost";
std::string masterServerAddr="lenss-comp1.cse.tamu.edu";
std::string masterConnectorPort="6004";

//Used by Master Process to store the address of PrimaryWorker
std::string prevP1 = "";
std::string prevP2 = "";

int serverID = 0;
int slaveServerStatus[3];	//0-master,1-server1,2-server2, this indicates if server is on
std::vector<std::vector<std::string>> heartBeatCandidate;
std::vector<ServerConnect*> dataConnectVect;
std::vector<ServerConnect*> localConnect;

ServerConnect* masterPrimaryWorker;

//current PrimaryWorker Info
struct primaryWorkerInfo{
    std::string hostname;
    int connected_clients;
}p_worker_info[2] = {{"",0},{"",0}};
    

//check if given username has connected to this server
bool checkConnected(std::string username){
    for(std::string str : connected_clients){
        if(username == str) return true;
    }
    return false;
}

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

//this returns server id with given port address
int find_server_ID(std::string addr){
	
	if(addr.compare("lenss-comp1.cse.tamu.edu:6004") <= 0) return 0;
	if(addr.compare("lenss-comp3.cse.tamu.edu:6007") <= 0) return 1;
	return 2;

}

//this detects if the given input address is leader 
//on the same machine
bool isLeaderDown(std::string addr){
    if(addr.compare(leaderAddr) == 0)
        return true;
    else
        return false;
}

void writeLeader1(std::string str){
	std::ofstream newfile("leader1.txt");
	newfile << str;
	newfile.close();
}

void writeLeader2(std::string str){
	std::ofstream newfile("leader2.txt");
	newfile << str;
	newfile.close();
}

std::string getLeader1(){
	std::string rv;
	std::fstream readfile("leader1.txt");
	std::getline(readfile, rv);
	readfile.close();
	return rv;
}

std::string getLeader2(){
	std::string rv;
	std::fstream readfile("leader2.txt");
	std::getline(readfile, rv);
	readfile.close();
	return rv;
}

std::string find_port(std::string addr){
	std::size_t pos = addr.find("60");
	return addr.substr(pos);
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
    
    const char *k = filename.c_str();
    rename(k, "del.txt");
    rename("in2.txt", k);
    remove("del.txt");//Delete temp txt
    return;
}

//this function is called when leader wants to restart a new slave process
void* startSlaveProc(void* destAddr){
	std::string dest_addr = *(static_cast<std::string*>(destAddr));
	std::string dest_port = find_port(dest_addr);
	std::string cmd = "./fbsd";
	if(dest_port.compare("6004") == 0 ||dest_port.compare("6007") == 0 ||dest_port.compare("6010") == 0){
		cmd += " -c 1";
	}
	cmd += " -p " + dest_port + " -h " + localHostName;
	if(localHostName.compare("masterServer") == 0) {
		cmd += " -m 1";
	}
	cmd += " -i " + std::to_string(serverID);
	printf("%s\n",cmd.c_str());
	
	if(std::system(cmd.c_str()) == -1) std::cout<< "failed when execute <" + cmd + ">" << std::endl;
}


 
//Class used for slave server to connect to master server
class ServerConnect{
public:
	ServerConnect(std::shared_ptr<Channel> channel, std::string server_Name, std::string local_Port_Name){
		serverStub = RegisterServer::NewStub(channel);
		serverName = server_Name;
		localPortName = local_Port_Name;
	}
	
	//each process call this function at least once to monitor other processe(s)
	std::string ProcHeartBeat(std::string destAddr){
		ClientContext context;
		ServerReply heartBeatRequest;
		ServerReply heartBeatReply;
		heartBeatRequest.set_leader(0);
		heartBeatRequest.set_message("?");
		heartBeatRequest.set_portnum(localPort);

		Status status = serverStub->ProcHeartBeat(&context, heartBeatRequest, &heartBeatReply);
		int dest_server_id = find_server_ID(destAddr);
		if(status.ok()) {
			
			if(dest_server_id == serverID && heartBeatReply.leader() == 1){
				leaderAddr = destAddr;
			}
			
			
			if(isMaster == true && isServerConnector == true){
				if(heartBeatReply.leader() == 1){
					std::ofstream newfile("leader"+std::to_string(dest_server_id)+".txt");
					newfile << destAddr;
					newfile.close();			
				}
			}

			slaveServerStatus[dest_server_id] = 1;
			return heartBeatReply.message();
		}
		else{

			if(isMaster == true && isServerConnector == true){
				slaveServerStatus[dest_server_id] = 0;
				sleep(5);
				
				if(slaveServerStatus[dest_server_id] == 0){
						std::ofstream newfile("leader"+std::to_string(dest_server_id)+".txt");
						newfile << " ";
						newfile.close();
				}
			}

			if(dest_server_id == serverID && isLeader == true){
				pthread_t startSlaveProc_id;
				pthread_create(&startSlaveProc_id, NULL, startSlaveProc, static_cast<void*>(&destAddr));
				sleep(1);
			}else if(dest_server_id == serverID && isLeader == false && isLeaderDown(destAddr) == true){
				startPrimaryProc(destAddr);
			}
		}
	}

	//each time the slave server is on, W7 and W10 would register the slave machine
	//so that the master machine would know that two slave mechines are turned on
	std::string ServerRegister(){
		ClientContext context;
		ServerReply slaveServerRequest;
		ServerReply masterServerReply;
		slaveServerRequest.set_message(serverName);
		slaveServerRequest.set_id(serverID);
		Status status = serverStub->ServerRegister(&context, slaveServerRequest, &masterServerReply);
		if(status.ok()) {
			return masterServerReply.message();	
		}
		std::cout<< "failed at Slave Server Register\n";
		abort();
	}

	//each time primary workers are created, they would cooperate with each other to complete the
	//process of election
	std::string Election(){
		ClientContext context;
		ServerReply requestElection;
		ServerReply replyElection;
		requestElection.set_message(localPort);
		requestElection.set_id(serverID);
		Status status = serverStub->Election(&context, requestElection, &replyElection);
		if(status.ok()){
			return 	replyElection.message();
		}
		std::cout<< "failed at election from "<< localPort << std::endl;
		abort();
	}

	//the function is called by newly elected leader to produce a slave process to take replace of 
	//previous one
	static void startPrimaryProc(std::string destAddr){

		std::string dest_addr = destAddr;
		if(!isServerConnector){
			//once on the same machine, we do election
			isLeader = true;
			if(find_server_ID(dest_addr) == serverID){
				for(int i = 0; i < localConnect.size(); i++){
					std::string election_connection = (heartBeatCandidate[serverID])[i];
					if(!(election_connection.compare(localServer+localPort))) continue;
					if(!(localConnect[i]->localPortName).compare(dest_addr)) continue;
					ServerConnect *server_connect = localConnect[i];
					std::string electionReply = server_connect->Election();
					if((electionReply).compare(localPort) > 0){ 
						
					}else{
						isLeader = false;
					}
				}
			}

			if(isLeader == true){
				LoadDatabase();
				std::cout << "leader " + localPort << " loaded database and is recreating a slave process to run at " << dest_addr << std::endl;
				pthread_t startSlaveProc_id;
				pthread_create(&startSlaveProc_id, NULL, startSlaveProc, static_cast<void*>(&destAddr));
				sleep(1);
			}
		}
	}

    
    //Synchronizing join function
    void Join(std::string username1, std::string username2){
		ClientContext context;
		DataSync primaryJoinRequest;
		ServerReply PrimaryUpdateReply;
		primaryJoinRequest.set_username(username1);
		primaryJoinRequest.set_targetname(username2);
        primaryJoinRequest.set_servername(localHostName);
		Status status = serverStub->Join(&context, primaryJoinRequest, &PrimaryUpdateReply);
		if(status.ok()) return;
		std::cout<< "failed at Synchronizing Database\n";
		abort();
    }
    
    //synchronizing Login function
    void Login(std::string username){
		ClientContext context;
		DataSync primaryLoginRequest;
		ServerReply PrimaryUpdateReply;
		primaryLoginRequest.set_username(username);
        primaryLoginRequest.set_servername(localHostName);
		Status status = serverStub->Login(&context, primaryLoginRequest, &PrimaryUpdateReply);
        
        //std::cout<<"Finished"<<std::endl;
		if(status.ok()) return;
		std::cout<< "failed at Synchronizing Database\n";
		abort();
    }
    
    void Leave(std::string username1, std::string username2){
		ClientContext context;
		DataSync primaryLeaveRequest;
		ServerReply PrimaryUpdateReply;
		primaryLeaveRequest.set_username(username1);
		primaryLeaveRequest.set_targetname(username2);
        primaryLeaveRequest.set_servername(localHostName);

		Status status = serverStub->Leave(&context, primaryLeaveRequest, &PrimaryUpdateReply);
        
        //std::cout<<"Finished"<<std::endl;
		if(status.ok()) return;

		std::cout<< "failed at Synchronizing Database\n";
		abort();
    }
    
    //update posts history
    void updateTimeLine(std::string post, std::string username){
		ClientContext context;
		DataSync primaryUpdateRequest;
		ServerReply PrimaryUpdateReply;
		primaryUpdateRequest.set_message(post);
		primaryUpdateRequest.set_username(username);
        primaryUpdateRequest.set_servername(localHostName);
		Status status = serverStub->updateTimeLine(&context, primaryUpdateRequest, &PrimaryUpdateReply);
		if(status.ok()) return;

		std::cout<< "failed at Synchronizing Timeline\n";
		abort();
    }
    
    //forward msgs to the follwers
    void msgForward(std::string msg, std::string username, std::string targetname){
		ClientContext context;
		DataSync primaryMsgRequest;
		ServerReply PrimaryUpdateReply;
        
		primaryMsgRequest.set_message(msg);
		primaryMsgRequest.set_username(username);
        primaryMsgRequest.set_targetname(targetname);
        primaryMsgRequest.set_servername(localHostName);
		Status status = serverStub->msgForward(&context, primaryMsgRequest, &PrimaryUpdateReply);
        
        //std::cout<<"Finished"<<std::endl;
		if(status.ok()) return;

		std::cout<< "failed at Synchronizing Timeline\n";
		abort();
    }
    std::string getName(){
        return localPortName;
    }
private:
	std::string serverName;
	std::string localPortName;
	std::unique_ptr<RegisterServer::Stub> serverStub;
};

class ServerConnectImpl final:public RegisterServer::Service{
	Status ProcHeartBeat(ServerContext* context, const ServerReply* request, ServerReply* reply) override {
		if(!(request->message().compare("?"))){
			if(isLeader == true){
				reply->set_leader(1);
			}
			reply->set_message(request->portnum()+"-->"+localPort);
			reply->set_id(serverID);
			reply->set_portnum(localPort);
		}
		return Status::OK;
	}
	Status ServerRegister(ServerContext* context, const ServerReply* request, ServerReply* reply) override {
		int request_server_id = (int)(request->id());
		if (slaveServerStatus[request_server_id] == 0){
			slaveServerStatus[request_server_id] = 1;
			std::string register_reply = request->message()+" Register Successful!";
			reply->set_message(register_reply);
			reply->set_id(0);
		}else{
			std::string register_reply = "Welcome back, "+ request->message();
			reply->set_message(register_reply);
			reply->set_id(0);
		}
		return Status::OK;
  	}
	Status Election(ServerContext* context, const ServerReply* request, ServerReply* reply) override {
		if(((int)(request->id())) == serverID){
			reply->set_message(localPort);
		}else{
			std::cout << "!!!request server is " << (int)(request->id()) <<", my server is serverID!!!" << std::endl;
		}
		return Status::OK;
	}
    
    
    Status msgForward(ServerContext* context, const DataSync* request, ServerReply* reply) override {
        std::string msg = request->message();
        std::string username = request->username();
        std::string targetname = request->targetname();
        
        Post post;
        post.set_username(username);
        post.set_content(msg);
        
        for(Client temp_client: client_db){
            if(temp_client.username == targetname && checkConnected(targetname) && temp_client.stream!=0 && temp_client.connect_status)
                temp_client.stream->Write(post);
        }
        
        
        //Propogate to other Primary Worker
        if(isMaster == true && isServerConnector==true){
            if(request->servername() == "server1"){
                std::string address = getLeader2();
                for(ServerConnect* s : dataConnectVect){
                    if(s->getName() == address){
                        s->msgForward(msg,username,targetname);
                    }
                }
            }
            if(request->servername() == "server2"){
                std::string address = getLeader1();
                for(ServerConnect* s : dataConnectVect){
                    if(s->getName() == address){
                        s->msgForward(msg,username,targetname);
                    }
                }
            }
        
        }
        
        return Status::OK;
    }

    Status updateTimeLine(ServerContext* context, const DataSync* request, ServerReply* reply) override {
      
      std::string msg = request->message();
      std::string username = request->username();
      
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
      const char *k = filename.c_str();
      rename(k, "del.txt");
      rename("in2.txt", k);
      remove("del.txt");//Delete temp txt
      
      //propagate to other Primaryserver
      if(isMaster == true && isServerConnector==true){
          if(request->servername() == "server1"){
              std::string address = getLeader2();
              for(ServerConnect* s : dataConnectVect){
                  if(s->getName() == address){
                      s->updateTimeLine(msg, username);
              }
             }
          }
          if(request->servername() == "server2"){
              std::string address = getLeader1();
              for(ServerConnect* s : dataConnectVect){
                  if(s->getName() == address){
                      s->updateTimeLine(msg, username);
              }
             }
          }
        
      }
      
      return Status::OK;
  }
  Status Join(ServerContext* context, const DataSync* request, ServerReply* reply) override {
      std::string username1 = request->username();
      std::string username2 = request->targetname();
      int join = find_user(username2);
      //If you try to join a non-existent client or yourself, send failure message
      if(join == -1|| username1 == username2)
        reply->set_message("Join Failed -- Username Not Exist or Own Username");
      else{
          Client *user2 = &client_db[join];
          Client *user1 = &client_db[find_user(username1)];
        //If user1 already join user2, send failure message
          for(Client* c: user2->joined){
              if(c->username == user1->username){
                      reply->set_message("Join Failed -- Alredy Joined This User");
                      return Status::OK;
              }
          }
        user2->joined.push_back(user1);
      
        UpdateDatabase(user2);
      
        if(isMaster == true && isServerConnector==true){
            if(request->servername() == "server1"){
                std::string address = getLeader2();
                for(ServerConnect* s : dataConnectVect){
                    if(s->getName() == address){
                        s->Join(username1, username2);
                    }
                }
            }
            if(request->servername() == "server2"){
                std::string address = getLeader1();
                for(ServerConnect* s : dataConnectVect){
                    if(s->getName() == address){
                            s->Join(username1, username2);
                    }
                }
            }
        
        }
      
        reply->set_message("Join Successful");
      }
      return Status::OK; 
  }
  
  Status Login(ServerContext* context, const DataSync* request, ServerReply* reply) override {
    Client c;
    std::string username = request->username();
    std::string filename = localHostName + username + ".txt";
    int idx = find_user(username);
    if(idx == -1){  // first timelogin
      c.username = username;
      client_db.push_back(c);
      if(!CheckFile(filename)){
          std::ofstream fout(filename,std::ios::out);
          fout.close();
      }
      
      UpdateDatabase(&c);
     
      if(isMaster == true && isServerConnector==true){
          if(request->servername() == "server1"){
              std::string address = getLeader2();
              for(ServerConnect* s : dataConnectVect){
                  if(s->getName() == address){
              s->Login(username);
          }
      }
          }
          if(request->servername() == "server2"){
              std::string address = getLeader1();
              for(ServerConnect* s : dataConnectVect){
                  if(s->getName() == address){
              s->Login(username);
          }
      }
          }
      
      } 
    }
    return Status::OK;
}
  
Status Leave(ServerContext* context, const DataSync* request, ServerReply* reply) override {
  std::string username1 = request->username();
  std::string username2 = request->targetname();
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
    for(Client* c: user2->joined){
        if(c->username == user1->username){
                user2->joined.erase(user2->joined.begin() + count); 
                
                UpdateDatabase(user2);
                
                if(isMaster == true && isServerConnector==true){
                    if(request->servername() == "server1"){
                        std::string address = getLeader2();
                        for(ServerConnect* s : dataConnectVect){
                            if(s->getName() == address){
                        s->Leave(username1, username2);
                    }
                }
                    }
                    if(request->servername() == "server2"){
                        std::string address = getLeader1();
                        for(ServerConnect* s : dataConnectVect){
                            if(s->getName() == address){
                        s->Leave(username1, username2);
                    }
                }
                    }
      
                }
                
                reply->set_message("Leave Successful");
                return Status::OK;
        }
        count++;
    }
  }
  reply->set_message("Leave Failed -- Not Joined Yet");
  return Status::OK;
}
    
};


//The stub functions let client call
//Communication with client
class FBChatServerImpl final : public FBChatServer::Service {
    
    //Master Server allocate avaliable Primary Worker for Client to connect.
    Status Connect(ServerContext* context, const ClientRequest* request, ServerReply* reply) override {
          std::cout<< "Master side" + request->username() <<std::endl;
          std::string primaryWorkerAddress;
          
          
          //allocate clent to a Primary Worker which connecting fewer clients
          p_worker_info[0].hostname = getLeader1();
          p_worker_info[1].hostname = getLeader2();
          
          if(prevP1 != "" && prevP1 != p_worker_info[0].hostname){
              p_worker_info[0].connected_clients = 0;
          }
          else if(prevP2 != "" && prevP2 != p_worker_info[1].hostname){
              p_worker_info[1].connected_clients = 0;
          }
          
          
          if(p_worker_info[1].hostname == " " || p_worker_info[0].hostname != " " && p_worker_info[0].connected_clients <= p_worker_info[1].connected_clients){
              primaryWorkerAddress = p_worker_info[0].hostname;
              p_worker_info[0].connected_clients++;
            
          }
          else if(p_worker_info[0].hostname == " " || p_worker_info[1].hostname != " " && p_worker_info[1].connected_clients <= p_worker_info[0].connected_clients){
              primaryWorkerAddress = p_worker_info[1].hostname;
              p_worker_info[1].connected_clients++;
          }
          
          prevP1 =  p_worker_info[0].hostname;
          prevP2 =  p_worker_info[1].hostname;
        reply->set_message(primaryWorkerAddress);
        return Status::OK;
      }
    
  //Sends the list of total rooms and joined rooms to the client
     Status List(ServerContext* context, const ClientRequest* request, ShowList* showlist) override {
        Client user = client_db[find_user(request->username())];
        for(Client c : client_db){
          showlist->add_all_clients(c.username);
          if(c.username == user.username) continue;
          for(Client *i: c.joined){
              if(i->username == user.username){
                  showlist->add_joined_clients(c.username);
              }
          }
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
        for(Client* c: user2->joined){
            if(c->username == user1->username){
                    reply->set_message("Join Failed -- Alredy Joined This User");
                    return Status::OK;
            }
        }
      user2->joined.push_back(user1);
      
      UpdateDatabase(user2);
      
      masterPrimaryWorker->Join(username1, username2);
      
      
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
      for(Client* c: user2->joined){
          if(c->username == user1->username){
                  user2->joined.erase(user2->joined.begin() + count); 
                  UpdateDatabase(user2);
                  
                  masterPrimaryWorker->Leave(username1, username2);
                  
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
      std::cout<< "Log in"<<std::endl;
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
      connected_clients.push_back(username);
      UpdateDatabase(&c);
      masterPrimaryWorker->Login(username);
    }
    else{ 
      Client *user = &client_db[idx];
      std::cout<< "tt1"<<std::endl;
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
        connected_clients.push_back(user->username);
        user->connect_status = true;
      }
    }
    return Status::OK;
  }

  Status Alive(ServerContext* context, const ClientRequest* request, ServerReply* reply) override {
      if(!isMaster && isLeader)
      return Status::OK;
  }
  
  Status Check(ServerContext* context, const ClientRequest* request, ServerReply* reply) override {
      if(isMaster && isLeader){
          reply->set_message("isMaster");
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
          masterPrimaryWorker->updateTimeLine(fileinput, username);
      //Send messages to all clients this client has been joined by using ServerReaderWriter
        for(Client* temp_client: c->joined){
            if(checkConnected(temp_client->username) && temp_client->stream!=0 && temp_client->connect_status)
                temp_client->stream->Write(post);
            //write to client's timeline history
            std::string temp_username = temp_client->username;
            ModifyTimeLine(fileinput,temp_username);
            
            masterPrimaryWorker->updateTimeLine(fileinput, temp_username);
            masterPrimaryWorker->msgForward(post.content(), post.username(),temp_username);
            
            }
        }
    }
    //If the client disconnected from Chat Mode, set connected to false
    int count = 0;
    for(std::string str : connected_clients){
        if(str == c->username){
                connected_clients.erase(connected_clients.begin() + count);
            }
        count++;
    }
    c->connect_status = false;
    c->stream = 0;
    return Status::OK;
  }
};


void connectSetup(){

	//process at 6007 & 6010 are responsible to register two slave servers		
	if(isMaster==false && isServerConnector==true){
		std::string server_register_connection = masterServerAddr+":"+masterConnectorPort;
		std::shared_ptr<Channel> server_register_channel = grpc::CreateChannel(server_register_connection,grpc::InsecureChannelCredentials());
		ServerConnect *server_connect = new ServerConnect(server_register_channel, localHostName, server_register_connection);
		std::string serverRegisterReply = server_connect->ServerRegister();
		std::cout << serverRegisterReply << std::endl;
	}
    
	//if the process is the Primary Worker
	//connect to the worker on the master server
	if(isMaster == false && isServerConnector == false){
		std::string masterPrimary = masterServerAddr + ":" + masterConnectorPort;
		std::shared_ptr<Channel> primary_channel = grpc::CreateChannel(masterPrimary,grpc::InsecureChannelCredentials());
		masterPrimaryWorker = new ServerConnect(primary_channel, localHostName,masterPrimary);
	}
	sleep(1);
	
}

//each process periodically send signals to detect if the
//process at target address is alive
void* heartBeatDetector(void* destAddr){
	std::string dest_addr = *(static_cast<std::string*>(destAddr));
	std::string heart_beat_connection = dest_addr;
	std::shared_ptr<Channel> heart_beat_channel = grpc::CreateChannel(heart_beat_connection,grpc::InsecureChannelCredentials());
	ServerConnect *server_connect = new ServerConnect(heart_beat_channel, localHostName, dest_addr);
	if(isMaster == true && isServerConnector == true && (find_server_ID(dest_addr) == 1 || find_server_ID(dest_addr) == 2)){
		dataConnectVect.push_back(server_connect);
	}
	localConnect.push_back(server_connect);
	while(true){
		sleep(3);
		std::string serverHeartBeatReply = server_connect->ProcHeartBeat(dest_addr);
	}
	return 0;
}

//process monitor other processes
void* runHeartBeat(void *invalid){
	if(isMaster == false) {
		for(int i=0; i < heartBeatCandidate[serverID].size(); i++){
			std::string candidate = (heartBeatCandidate[serverID])[i];
			if(!candidate.compare(localServer+localPort))	continue;
			pthread_t thread_id;
			pthread_create(&thread_id, NULL, &heartBeatDetector, static_cast<void*>(&candidate));
			sleep(1);
		}
	}

	if(isMaster == true && isServerConnector == true){
		for(int i = 0; i < heartBeatCandidate[1].size(); i++){
			std::string candidate = (heartBeatCandidate[1])[i];
			pthread_t thread_id;
			pthread_create(&thread_id, NULL, &heartBeatDetector, static_cast<void*>(&candidate));
			sleep(1);
		}

		for(int i = 0; i < heartBeatCandidate[2].size(); i++){
			std::string candidate = (heartBeatCandidate[2])[i];
			pthread_t thread_id;
			pthread_create(&thread_id, NULL, &heartBeatDetector, static_cast<void*>(&candidate));
			sleep(1);
		}
	}
	
	if(isMaster == true && isServerConnector == false){	
		for(int i=0; i < heartBeatCandidate[0].size(); i++){
			std::string candidate = (heartBeatCandidate[0])[i];
			if(!candidate.compare(localServer+localPort))	continue;
			pthread_t thread_id;
			pthread_create(&thread_id, NULL, &heartBeatDetector, static_cast<void*>(&candidate));
			sleep(1);
		}
	}	

	return 0;
}



//all the processes have a chance to be beat monitored
void* ListenHeartBeat(void* invalid){
	std::string server_address = localServer+localPort;
	ServerConnectImpl serverConnectService;
    	FBChatServerImpl service;
    
	ServerBuilder builder;
	builder.AddListeningPort(server_address,grpc::InsecureServerCredentials());
    
	//Primary worker or Master should connect with client
	if((!isMaster) || (isMaster && !isServerConnector)) builder.RegisterService(&service);
	builder.RegisterService(&serverConnectService);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "port: " << localPort << " is listening with process id:" << getpid() <<std::endl;
	server->Wait();
}

int main(int argc, char** argv) {


	std::vector<std::string> server0;
	std::vector<std::string> server1;
	std::vector<std::string> server2;
	server0.push_back("lenss-comp1.cse.tamu.edu:6001");
	server0.push_back("lenss-comp1.cse.tamu.edu:6002");
	server0.push_back("lenss-comp1.cse.tamu.edu:6003");
	server0.push_back("lenss-comp1.cse.tamu.edu:6004");
	server1.push_back("lenss-comp3.cse.tamu.edu:6005");
	server1.push_back("lenss-comp3.cse.tamu.edu:6006");
	server1.push_back("lenss-comp3.cse.tamu.edu:6007");
	server2.push_back("lenss-comp4.cse.tamu.edu:6008");
	server2.push_back("lenss-comp4.cse.tamu.edu:6009");
	server2.push_back("lenss-comp4.cse.tamu.edu:6010");
	heartBeatCandidate.push_back(server0);
	heartBeatCandidate.push_back(server1);
	heartBeatCandidate.push_back(server2);



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
	if(isServerConnector == 1){
		slaveServerStatus[0] = 1;	
		slaveServerStatus[serverID] = 1;
	}

	if(isMaster == true && isLeader == true){
		for(int i = 0; i < heartBeatCandidate.size(); i++){
			std::ofstream out("leader"+std::to_string(i)+".txt");
			out << (heartBeatCandidate[i])[0];
			out.close();
		}
	}

	if(serverID == 0)
		localServer="lenss-comp1.cse.tamu.edu:";
	else if(serverID == 1)
		localServer="lenss-comp3.cse.tamu.edu:";
	else if(serverID == 2)
		localServer="lenss-comp4.cse.tamu.edu:";
	else
		std::cout << "your server id is not correct!" << std::endl;
	std::cout << localServer;

	pthread_t thread_id, heartBeatThread_id, runServerThread_id ;
	pthread_create(&thread_id, NULL, ListenHeartBeat, (void*) NULL);


	connectSetup();		//this function is mainly set for worker7 and worker10
	sleep(1);

	int rc = pthread_create(&heartBeatThread_id, NULL, runHeartBeat, (void*) NULL);
	sleep(1);


	//cheack if database file is existed or not
	std::string filename = localHostName + "client_database.txt";
		if(!CheckFile(filename)){
		std::ofstream fout(filename,std::ios::out);
		fout.close();
	}


	LoadDatabase();
	std::cout << "Loading Successful" << std::endl;

	(void)pthread_join(thread_id, NULL);
	(void)pthread_join(heartBeatThread_id, NULL);
	return 0;
}
