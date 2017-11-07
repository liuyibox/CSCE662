/*a simple gRPC client*/

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "fb.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using assignment2::Post;
using assignment2::ShowList;
using assignment2::ClientRequest;
using assignment2::ServerReply;
using assignment2::FBChatServer;
using namespace std;

string possibleMaster[3] = {"localhost:6001", "localhost:6002", "localhost:6003"};
bool alive = false;// cheack is the connected PrimaryWorker is alive;

//Helper function used to create a Message object given a username and message
Post msg_setup(string username, string msg) {

	Post message;
	google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
	timestamp->set_seconds(time(NULL));
	timestamp->set_nanos(0);
	message.set_allocated_timestamp(timestamp);


	message.set_content(msg);
	message.set_username(username);

	return message;
}


class Client {

public:
Client(shared_ptr<Channel> channel, string name){

	clientStub = FBChatServer::NewStub(channel);
	username = name;
}


//list out the existing rooms exist and joining rooms
void List(string username){

	ClientContext context;
	ShowList reply;
	ClientRequest request;
	request.set_username(username);
	Status status = clientStub->List(&context, request, &reply);

	if(status.ok()){

        	cout << "existing rooms: \n";
        	for(string room : reply.all_clients()){
			cout << room << endl;
        	}

        	cout << "joining rooms: \n";
        	for(string room : reply.joined_clients()){
			cout << room << endl;;
		}
		
		return;
	}

	//if list failed
	cout << "failed at list\n";
}

//user1's update would display in user2's screen
void Join(string username1, string username2){

	ClientContext context;
	ServerReply reply;
	ClientRequest request;
	request.set_username(username1);
	request.add_requestinfo(username2);

	Status status = clientStub->Join(&context, request, &reply);

	if(status.ok()){
		cout << reply.message() << endl;
		return;
	}
	//if join failed
	cout << "failed at join\n";
	abort();
}

  //user2's update would not display in user1's screen
void Leave(string username1, string username2){

	ClientContext context;
	ServerReply reply;
	ClientRequest request;
	request.set_username(username1);
	request.add_requestinfo(username2);

	Status status = clientStub->Leave(&context, request, &reply);
	if(status.ok()){
		cout << reply.message() << endl;
		return;
	}

	//if leave failed
	cout << "failed at leave\n";
	abort();
}

//connect to master first
string Connect(string uername){
	ClientContext context;
	ServerReply reply;
	ClientRequest request;  
 
	request.set_username(username);

	Status status = clientStub->Connect(&context, request, &reply);

	if(status.ok()) return reply.message();
    cout << status.error_details()<< endl;
	//if Connect failed
	cout<< "failed at connecting to server\n";
	abort();
}

//user login
string Login(string username){

	ClientContext context;
	ServerReply reply;
	ClientRequest request;  
	request.set_username(username);

	Status status = clientStub->Login(&context, request, &reply);
	if(status.ok()) return reply.message();
    
	//if login failed
	cout<< "failed at login\n";
	abort();
}

void Chat (string username) {
	ClientContext context;

	//used to establish the read and write between client and server
	shared_ptr<ClientReaderWriter<Post, Post>> read_write(clientStub->Chat(&context));

	//writer reads from command line and send to the server
	thread writer(
		[username, read_write]() {  
			string initial_input = "20";
			Post initial_msg = msg_setup(username, initial_input);
			read_write->Write(initial_msg);

			cout << "======================You are now in chat mode=======================\n";
			string chat_input;
			Post chat_msg;

			
			while(getline(cin, chat_input)){
                
				chat_msg = msg_setup(username, chat_input);
				read_write->Write(chat_msg);
        		}
	        	read_write->WritesDone();
		}
	);

	//through the stream between server and client, reader thread keep reading
	thread reader(
		[username, read_write]() {
			Post msg;
			while(read_write->Read(&msg)){
				cout << msg.username() << " posted \"" << msg.content() << "\""  << endl;
			}
		}
	);

	//Wait for the threads to finish
	writer.join();
	reader.join();
}

//check if primary worker is alive
void Alive(){
	ClientContext context;
	ServerReply reply;
	ClientRequest request;  

	Status status = clientStub->Alive(&context, request, &reply);
	if(status.ok()) return;
    
	//if not alive
	cout<< "Primary Worker Dead" <<endl;
    cout<< "Please Press Enter to ReConnect" <<endl;
	alive = false;
}

//check if it is the master
string Check(){
	ClientContext context;
	ServerReply reply;
	ClientRequest request;  

	Status status = clientStub->Check(&context, request, &reply);
    cout<< reply.message() <<endl;
	if(status.ok() && reply.message() == "isMaster") return "Master";
    else return "Neg";
	//if not alive
	
}

private:
	string username;
	unique_ptr<FBChatServer::Stub> clientStub;
};


Client *connect_to_master;
Client *connect_to_server;

//in the while loop to receive command line msgs
string command_exe(Client* connect, string username, string user_input){

	//if the input command is "LIST" or "CHAT"
	if(user_input == "LIST"){
		connect->List(username); 
		return "LIST";
	}else if(user_input == "CHAT"){
		return "CHAT";
	}
	
	//we extract the command and the arguments from command line separately
	size_t command_pos = user_input.find_first_of(" ");
	string user_command = user_input.substr(0, command_pos);
	string user_argument = user_input.substr(command_pos+1, (user_input.length()-command_pos));

	//if the command is "JOIN" or "LEAVE"
	if(user_command == "JOIN"){
		connect->Join(username, user_argument);
	}else if(user_command == "LEAVE")
	      connect->Leave(username, user_argument);
	else{
     		return "NO THIS COMMAND";   
	}

	return " ";   
}

//check if Primary Worker is Alive
void *checkAlive(void *ptr){
    while(alive){
        sleep(1);
        connect_to_server->Alive();
    }
}

int main(int argc, char** argv) {

	string hostname, port_number, username;
	if(argc != 4) {
		cout << "Usage: ./fbc <hostname> <port> <username>\n";
		abort();
	}else{
		hostname = string(argv[1]);
		port_number = string(argv[2]);
		username = string(argv[3]);

		size_t pos = username.find(" ");
		if(pos != string::npos){
			cout << "your username contains a \" \"\n";
			abort();		
		}
	
		pos = username.find("~");
		if(pos != string::npos){
			cout << "your username contains a \"~\"\n";
			abort();
		}
	}
    while(1){
        
    bool isMaster = false;
        
    while(!isMaster){
        for(string info: possibleMaster){
        string info_connection = info;
        //setup the channel from client to server
        shared_ptr<Channel> channel_master = grpc::CreateChannel(info_connection, grpc::InsecureChannelCredentials());
        //create a client object to connect to master
        connect_to_master = new Client(channel_master, username);
        string reply = connect_to_master->Check();
            if(reply == "Master"){
                isMaster = true;
                break;
            }
        }
    }
    
    cout << "Connecting to Master \n";
    string primary_worker_address = connect_to_master->Connect(username);
    cout << primary_worker_address << endl;
    
    
    //get the port of Primary Worker and connect to the Primary Worker
    cout << "Redirecting to Primary Worker\n";
    shared_ptr<Channel> channel_primary = grpc::CreateChannel(primary_worker_address, grpc::InsecureChannelCredentials());
    
    connect_to_server = new Client(channel_primary, username);
    
	//request to login
	string login_reply = connect_to_server->Login(username);

	//If the username already exists, exit the client
	if(login_reply == "Invalid Username"){
		cout << "This user is already connected \n";
		return 0;
	}
    
    
  
	cout << login_reply << endl;
	cout << "====================You are now in command mode======================\n" ;
    
    
    //create a thread to check primary is alive?
    alive = true;
    pthread_t tid;
    pthread_create(&tid, NULL, checkAlive, NULL);
    
	string command_line_input; 
	while(alive && getline(cin, command_line_input)){

		//check client choose to enter into chat mode
		string s = command_exe(connect_to_server, username, command_line_input);

        	if(s == "CHAT")	break;
		if(alive && s == "NO THIS COMMAND") cout << "NO THIS COMMAND" << endl;
	}
	//enter chat mode out of the while loop above
	 if(alive) connect_to_server->Chat(username);
    }
	return 0;
}
