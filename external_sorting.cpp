#include <vector>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <iterator>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <regex.h>
using namespace std;


int buffer_pages,keys_per_page,buffer_per_io,total_keys;
vector<vector<int> > input_runs;
vector<vector<int> > output_runs;

typedef struct Node
{
	int key;
	int input_run_id;
}HeapNode;
void swap(HeapNode *a, HeapNode *b){

	HeapNode temp = *a;
	*a = *b;
	*b = temp;
}

void print_vector(vector<int> v){
	int size = v.size();
	cout << "Vector Size = " << size << endl;

	for (int i = 0; i < size; ++i)
	{
		cout << v[i] << ", ";
	}
	cout << "\b\b " << endl;
}

void print_vector(vector<HeapNode> v){
	int size = v.size();
	for (int i = 0; i < size; ++i)
	{
		//cout << v[i].key << " " << v[i].input_run_id << ", ";
		cout << v[i].key << ", ";

	}
	cout << "\b\b " << endl;
}
void print_2d_array(vector<vector<int> >& matrix){
	vector<vector<int> >::iterator outer = matrix.begin();
	
	while(outer != matrix.end()){

		vector<int>::iterator inner = outer->begin();
		while(inner != outer->end()){
			cout << *inner << ", ";
			inner++;
		}
		cout << "\b\b"<<" "<<endl;
		outer++;
	}
}
void print_range_vector(vector<vector<int> >& matrix,int start,int end){
	vector<vector<int> >::iterator outer = matrix.begin() + start;
	
	while(outer != matrix.begin() + end){
		vector<int>::iterator inner = outer->begin();
		while(inner != outer->end()){
			cout << *inner << ", ";
			inner++;
		}
		cout << "\b\b"<<" "<<endl;
		outer++;
	}
}

int checkint(const char *str){
	regex_t regex;
	int reti = regcomp(&regex, "^[0-9]*$", 0);
	if (reti) {
	    //fprintf(stderr, "Could not compile regex\n");
	    fprintf(stderr, "Check Command Line Arguments\n");
	    exit(1);
	}

	/* Execute regular expression */
	reti = regexec(&regex, str, 0, NULL, 0);
	if (!reti) {
	    //puts("Match");
	    return 1;
	}
	else if (reti == REG_NOMATCH) {
	   // puts("No match");
	    return -1;
	}
	else {
	    
	    return -1;
	}
}

class Buffers
{
	public:
	int mpass_id;
	vector<vector<int> > minput_runs;
	int mstart;
	int mend;
	vector<vector<int> > minput_buffers;
	vector<int> page_faults;
	vector<int> moutput_buffer;
	int moutput_buffer_id;
	vector<int> *moutput_run;


	Buffers(vector<vector<int> >& input_runs,int start,int end,vector<int>& output_run,int passi);
	//~Buffers();

	
};

Buffers::Buffers(vector<vector<int> >& input_runs,int start,int end,vector<int>& output_run,int passi){
	minput_runs = input_runs;
	mstart = start;
	mend = end;
	mpass_id = passi;
	moutput_buffer_id = 0;
	int i = start;
	moutput_run = &output_run;

	for (int t = 0; t < mend-mstart; ++t)
	{
		page_faults.push_back(0);
	}
	
	while(i < end){

		//page_faults.push_back(0);
		int size;
		if (input_runs[i].size() < buffer_per_io*keys_per_page)
		{
			size = input_runs[i].size();
		}
		else{
			size = buffer_per_io*keys_per_page;
		}
		vector<int> temp;

		//int page_fault = 0;//size/keys_per_page;

		for (int j = 0; j < size; ++j)
		{
			temp.push_back(minput_runs[i][0]);
			minput_runs[i].erase(minput_runs[i].begin());
			if (j%keys_per_page == 0)
			{
				if (mpass_id != 0)
				{
					
					cout << "Read page fault for R" << passi-1 << "_" << i<< " Page[" << page_faults[i - mstart] << "]" << endl;
				}
				page_faults[i - mstart]++;
			}

		}
		minput_buffers.push_back(temp);
		i++;
	}
	//minput_runs = input_runs;

}

class MinHeap
	{
	public:
		vector<HeapNode> harr;
		int heap_size;
		MinHeap(Buffers *main_memory);
		//~MinHeap();
		//insert();
		int left(int i) { return 2*i + 1;}
		int right(int i) {return 2*i + 2;}
		void heapify(int i);
		void getMin(Buffers *main_memory);
		void replaceMin(Buffers *main_memory);
		
		
};
MinHeap::MinHeap(Buffers *main_memory){

	int total_input_runs = main_memory->mend - main_memory->mstart;
	int i = 0;
	
	while(i < total_input_runs){
		if (main_memory->minput_buffers[i].size() != 0)
		{
			HeapNode temp;
			temp.key = main_memory->minput_buffers[i][0];
			//main_memory->minput_buffers[i].erase(main_memory->minput_buffers[i].begin());
			temp.input_run_id = i;
			harr.push_back(temp);
		}
		i++;
	}

	i = (harr.size() - 1)/2;
	while(i >= 0){
		heapify(i);
		i--;
	}
}

void MinHeap::heapify(int i){

	int l = left(i);
	int r = right(i);
	int smallest = i;
	if (l < harr.size() && harr[l].key < harr[i].key )
	{
		smallest = l;
	}
	if (r < harr.size() && harr[r].key < harr[smallest].key )
	{
		smallest = r;
	}

	if(smallest != i){
		swap(&harr[i],&harr[smallest]);
		heapify(smallest);
	}
}

void MinHeap::replaceMin(Buffers *main_memory){

	int i = harr[0].input_run_id;
	/*if (harr.size() == 1){
		//cout << "end of a batch" << endl;
		harr.pop_back();
		return;
	}*/
	if (main_memory->minput_buffers[i].size() != 0)
	{
		//cout << "Erasing first key in input buffes" << endl;
		main_memory->minput_buffers[i].erase(main_memory->minput_buffers[i].begin());
		
	}
	if (main_memory->minput_buffers[i].size() == 0)
	{
		if (main_memory->minput_runs[i + main_memory->mstart].size() != 0)
		{
			//printf("!!!!!!!!\n");
			
			int size;
			if (main_memory->minput_runs[i + main_memory->mstart].size() < buffer_per_io*keys_per_page)
			{
				size = main_memory->minput_runs[i + main_memory->mstart].size();
			}
			else{
				size = buffer_per_io*keys_per_page;
			}
			//int page_fault = 0;//size/keys_per_page;
			for (int j = 0; j < size; ++j)
			{
				main_memory->minput_buffers[i].push_back(main_memory->minput_runs[i + main_memory->mstart][0]);
				main_memory->minput_runs[i + main_memory->mstart].erase(main_memory->minput_runs[i + main_memory->mstart].begin());
				if (j%keys_per_page == 0)
				{
					if (main_memory->mpass_id != 0)
					{
						
					cout << "Read page fault for R" << main_memory->mpass_id -1 << "_" << i + main_memory->mstart<< " Page[" << main_memory->page_faults[i] << "]" << endl;
					}
					main_memory->page_faults[i]++;
				}

			}

			HeapNode temp;
			temp.key = main_memory->minput_buffers[i][0];
			//main_memory->minput_buffers[i].erase(main_memory->minput_buffers[i].begin());
			temp.input_run_id = i;
			harr[0] = temp;
			heapify(0);
			return;
		}
		else{
			/*int total_input_buffers = main_memory->mend - main_memory->mstart;
			int j= 0;
			while(j < total_input_buffers){
				
				if (main_memory->minput_buffers[j].size() >= 1)
				{
					HeapNode temp;
					temp.key = main_memory->minput_buffers[j][1];
					main_memory->minput_buffers[j].erase(main_memory->minput_buffers[j].begin());
					temp.input_run_id = j;
					harr[0] = temp;
					heapify(0);
					return;
				}
				j++;
			}
			*/
			if (harr.size() != 0)
			{
				harr[0] = harr[harr.size() - 1];
				harr.pop_back();
				heapify(0);
					
				return;
			}
			else{
				return;
			}

		}
	}
	else{
			HeapNode temp;
			temp.key = main_memory->minput_buffers[i][0];
			//main_memory->minput_buffers[i].erase(main_memory->minput_buffers[i].begin());
			temp.input_run_id = i;
			harr[0] = temp;
			heapify(0);
			if (main_memory->minput_buffers[i].size() == 0)
			{
				if (main_memory->minput_runs[i + main_memory->mstart].size() != 0)
				{
					/*printf("@@@@@@@\n");
					print_vector(main_memory->minput_runs[i]);
					printf("@@@@@@@\n");*/
					int size;
					if (main_memory->minput_runs[i + main_memory->mstart].size() < buffer_per_io*keys_per_page)
					{
						size = main_memory->minput_runs[i + main_memory->mstart].size();
					}
					else{
						size = buffer_per_io*keys_per_page;
					}
					
					//int page_fault = 0;//size/keys_per_page;
					for (int j = 0; j < size; ++j)
					{
						main_memory->minput_buffers[i].push_back(main_memory->minput_runs[i + main_memory->mstart][0]);
						main_memory->minput_runs[i + main_memory->mstart].erase(main_memory->minput_runs[i + main_memory->mstart].begin());
						if (j%keys_per_page == 0)
						{
							if (main_memory->mpass_id != 0)
							{
								
							cout << "Read page fault for R" << main_memory->mpass_id -1 << "_" << i + main_memory->mstart<< " Page[" << main_memory->page_faults[i] << "]" << endl;
								
							}
							main_memory->page_faults[i]++;
						}
					}
					
				}
			}
			return;
	}


}

void MinHeap::getMin(Buffers *main_memory){

	if (harr.size() == 0 && main_memory->moutput_buffer.size() > 0)
	{
		if (main_memory->mpass_id != 0)
		{
			
			cout << "Write page fault for output Page [" << main_memory->moutput_buffer_id << "]" << endl;
		}
		//main_memory->moutput_buffer.push_back(harr[0].key);
		main_memory->moutput_run->insert(main_memory->moutput_run->end(),main_memory->moutput_buffer.begin(),main_memory->moutput_buffer.end());
		main_memory->moutput_buffer.erase(main_memory->moutput_buffer.begin(),main_memory->moutput_buffer.end());
		//harr.erase(harr.begin());
		return;
		
	}
	int temp = harr[0].key;
	replaceMin(main_memory);
	main_memory->moutput_buffer.push_back(temp);
	if (main_memory->moutput_buffer.size() == keys_per_page)
	{
		//cout << "Write page fault for output Page [" << main_memory->moutput_buffer_id << "]" << endl;
		if (main_memory->mpass_id != 0)
		{
			
			cout << "Write page fault for output Page [" << main_memory->moutput_buffer_id << "]" << endl;
		}
		main_memory->moutput_run->insert(main_memory->moutput_run->end(),main_memory->moutput_buffer.begin(),main_memory->moutput_buffer.end());
		main_memory->moutput_buffer.erase(main_memory->moutput_buffer.begin(),main_memory->moutput_buffer.end());

		main_memory->moutput_buffer_id++;
		main_memory->moutput_buffer_id %= buffer_per_io;
	}
	
}

int  input_runs_pass0(){
	return total_keys;
}


int  input_runs_for_one_output_run_passi(int i){
	if (i == 0)
	{
		return buffer_pages*keys_per_page;
	}
	else{
		return (buffer_pages/buffer_per_io) - 1;
	}
}

int  num_output_runs(int passi){
	if (passi == 0)
	{
		return ceil((double)total_keys/(buffer_pages*keys_per_page));
	}
	else{
		return ceil((double)num_output_runs(passi-1)/input_runs_for_one_output_run_passi(passi));
	}
}


// Used to merge two sorted vectors(used only for pass0)
vector<int> merge_sorted(vector<int>& first,vector<int>& second){
	
	vector<int> final;
	vector<int>::iterator ifirst = first.begin();
	vector<int>::iterator isecond = second.begin();

	while(ifirst != first.end() && isecond != second.end()){
		if(*ifirst <= *isecond){
			final.push_back(*ifirst);
			ifirst++;
		}
		else{
			final.push_back(*isecond);
			isecond++;
		}
		

	}

	if(ifirst != first.end()){
		while(ifirst != first.end()){
			final.push_back(*ifirst);
			ifirst++;
		}
	}
	else{
		while(isecond != second.end()){
			final.push_back(*isecond);
			isecond++;
		}
	}
	return final;

}

// Used to merge more then 2 sorted vectors(used only for pass0)
// Recursively merge two sorted vectors
vector<int> merge_k_sorted(vector<vector<int> >& input_runs,int start,int end){
	
	
	vector<int> temp = input_runs[start];
	/*cout << "Merging Following..." << endl;
	print_vector(input_runs[start]);*/
	for (int i = start + 1; i < end; ++i)
	{	
		//print_vector(input_runs[i]);
		temp = merge_sorted(temp,input_runs[i]);
	}
	/*cout << "Merge Result:" << endl;
	print_vector(temp);*/
	return temp;	
}
vector<int> merge_k_sorted2(vector<vector<int> >& input_runs,int start,int end,int passi){

	vector<int> output_run;
	/*if (end - start == 0)
	{
		return NULL;
	}*/
	if (end - start == 1)
	{
		output_run.insert(output_run.end(),input_runs[start].begin(),input_runs[start].end());
		return output_run;

	}
	else{
		Buffers main_memory(input_runs, start, end,output_run,passi);
		//Buffers::MinHeap heap = new Buffers::MinHeap(main_memory);

		
		MinHeap heap(&main_memory);
		

		while(heap.harr.size() != 0 || main_memory.moutput_buffer.size() != 0){
			//printf("!!!!!!!\n");
			//print_vector(heap.harr);
			heap.getMin(&main_memory);
			
		}
		
	}
	
}

// Function to examine execution of the program
vector<int> merge_k_sorted3(vector<vector<int> >& input_runs,int start,int end,int passi){

	vector<int> output_run;

	if (end - start == 1)
	{
		output_run.insert(output_run.end(),input_runs[start].begin(),input_runs[start].end());
		return output_run;

	}
	else{
		Buffers main_memory(input_runs, start, end,output_run,passi);
		//Buffers::MinHeap heap = new Buffers::MinHeap(main_memory);

		
		MinHeap heap(&main_memory);
		printf("First Heap\n");
		print_vector(heap.harr);
		printf("Status of input buffers\n");
		print_2d_array(main_memory.minput_buffers);
		
		int i=0;
		while(heap.harr.size() != 0 || main_memory.moutput_buffer.size() != 0){
			//printf("!!!!!!!\n");
			//print_vector(heap.harr);
			//heap.getMin(&main_memory);
			printf("Calling %d getMIn\n",i++);
			heap.getMin(&main_memory);
			printf("Status of input runs\n");
			print_range_vector(main_memory.minput_runs,start,end);
			printf("Status of input buffers\n");
			print_2d_array(main_memory.minput_buffers);
			printf("Status of output buffers\n");
			print_vector(main_memory.moutput_buffer);
			printf("Status of output run\n");
			print_vector(*(main_memory.moutput_run));
			printf("Status of heap\n");
			print_vector(heap.harr);
			
		}
	}

}

// Funtion for ith pass whitout showing output
void passi(int i){
	int input_runs_per_output_run = input_runs_for_one_output_run_passi(i);
	
	if (i == 0)
	{
		/*cout << "Input Runs for Pass "<< i << " Total Input Runs For This Pass:" <<input_runs.size()<<endl;
		print_2d_array(input_runs);*/
		//vector<int>::iterator i = input_runs.begin();
		output_runs.erase(output_runs.begin(),output_runs.end());
		/*cout << "\n----------" << endl;*/
		for (int j = 0,k = 0; j < num_output_runs(i); ++j, k += input_runs_per_output_run)
		{
				int start = k;
				int end = k + input_runs_per_output_run;
				if(end > input_runs.size()){
					//cout << "0000000" << endl;
					end = input_runs.size();
				}
				/*cout << "Batch " << j << "	Input Runs:" <<endl;
				print_range_vector(input_runs,start,end);*/
				output_runs.push_back(merge_k_sorted(input_runs,start,end));
				/*cout << "Output Run:" << endl;
				print_vector(output_runs[j]);
				cout << "\n" << endl;*/

				
			
		}
		/*cout << "----------\n" << endl;

		cout << "Output Runs for Pass "<< i   << " Total Output Runs For This Pass:" << output_runs.size()<< endl << endl;	
		print_2d_array(output_runs);
		cout << "-------------------------------------------\n\n" << endl;*/
		return;
	}
	if (i % 2 == 0)
	{
		/*cout << "Input Runs for Pass "<< i << " Total Input Runs For This Pass:" <<input_runs.size()<<endl;
		print_2d_array(input_runs);*/
		//vector<int>::iterator i = input_runs.begin();
		output_runs.erase(output_runs.begin(),output_runs.end());
		/*cout << "\n----------" << endl;*/
		for (int j = 0,k = 0; j < num_output_runs(i); ++j, k += input_runs_per_output_run)
		{
				int start = k;
				int end = k + input_runs_per_output_run;
				if(end > input_runs.size()){
					//cout << "0000000" << endl;
					end = input_runs.size();
				}
				/*cout << "Batch " << j << "	Input Runs:" <<endl;
				print_range_vector(input_runs,start,end);*/
				output_runs.push_back(merge_k_sorted2(input_runs,start,end,i));
				/*cout << "Output Run:" << endl;
				print_vector(output_runs[j]);
				cout << "\n" << endl;*/

				
			
		}
		/*cout << "----------\n" << endl;

		cout << "Output Runs for Pass "<< i   << " Total Output Runs For This Pass:" << output_runs.size()<< endl << endl;	
		print_2d_array(output_runs);
		cout << "-------------------------------------------\n\n" << endl;*/
	}
	else
	{
		/*cout << "Input Runs for Pass "<< i  << " Total Input Runs For This Pass:" <<output_runs.size()<< endl;
		print_2d_array(output_runs);*/
		//vector<int>::iterator i = input_runs.begin();
		input_runs.erase(input_runs.begin(),input_runs.end());
		/*cout << "\n----------" << endl;*/
		for (int j = 0,k = 0; j < num_output_runs(i); ++j, k += input_runs_per_output_run)
		{
				int start = k;
				int end = k + input_runs_per_output_run;
				if(end > output_runs.size()){
					//cout << "0000000" << endl;
					end = output_runs.size();
				}
				/*cout << "Batch " << j << "	Input Runs:" <<endl;
				print_range_vector(output_runs,start,end);*/
				input_runs.push_back(merge_k_sorted2(output_runs,start,end,i));
				/*cout << "Output Run:" << endl;
				print_vector(input_runs[j]);
				cout << "\n" << endl;*/
				
			
		}
		/*cout << "----------\n" << endl;

		cout << "Output Runs for Pass "<< i  << " Total Output Runs For This Pass:" << input_runs.size()<< endl << endl;	
		print_2d_array(input_runs);
		cout << "-------------------------------------------\n\n" << endl;*/

	}

}


// Same as passi but shows output in detail at every stage of the sorting
void passicopy(int i){
	int input_runs_per_output_run = input_runs_for_one_output_run_passi(i);
	
	if (i == 0)
	{
		cout << "Input Runs for Pass "<< i << " Total Input Runs For This Pass:" <<input_runs.size()<<endl;
		print_2d_array(input_runs);
		//vector<int>::iterator i = input_runs.begin();
		output_runs.erase(output_runs.begin(),output_runs.end());
		cout << "\n----------" << endl;
		for (int j = 0,k = 0; j < num_output_runs(i); ++j, k += input_runs_per_output_run)
		{
				int start = k;
				int end = k + input_runs_per_output_run;
				if(end > input_runs.size()){
					//cout << "0000000" << endl;
					end = input_runs.size();
				}
				cout << "Batch " << j << "	Input Runs:" <<endl;
				print_range_vector(input_runs,start,end);
				output_runs.push_back(merge_k_sorted(input_runs,start,end));
				cout << "Output Run:" << endl;
				print_vector(output_runs[j]);
				cout << "\n" << endl;

				
			
		}
		cout << "----------\n" << endl;

		cout << "Output Runs for Pass "<< i   << " Total Output Runs For This Pass:" << output_runs.size()<< endl << endl;	
		print_2d_array(output_runs);
		cout << "-------------------------------------------\n\n" << endl;
		return;
	}
	if (i % 2 == 0)
	{
		cout << "Input Runs for Pass "<< i << " Total Input Runs For This Pass:" <<input_runs.size()<<endl;
		print_2d_array(input_runs);
		//vector<int>::iterator i = input_runs.begin();
		output_runs.erase(output_runs.begin(),output_runs.end());
		cout << "\n----------" << endl;
		for (int j = 0,k = 0; j < num_output_runs(i); ++j, k += input_runs_per_output_run)
		{
				int start = k;
				int end = k + input_runs_per_output_run;
				if(end > input_runs.size()){
					//cout << "0000000" << endl;
					end = input_runs.size();
				}
				cout << "Batch " << j << "	Input Runs:" <<endl;
				print_range_vector(input_runs,start,end);
				output_runs.push_back(merge_k_sorted3(input_runs,start,end,i));
				cout << "Output Run:" << endl;
				print_vector(output_runs[j]);
				cout << "\n" << endl;

				
			
		}
		cout << "----------\n" << endl;

		cout << "Output Runs for Pass "<< i   << " Total Output Runs For This Pass:" << output_runs.size()<< endl << endl;	
		print_2d_array(output_runs);
		cout << "-------------------------------------------\n\n" << endl;
	}
	else
	{
		cout << "Input Runs for Pass "<< i  << " Total Input Runs For This Pass:" <<output_runs.size()<< endl;
		print_2d_array(output_runs);
		//vector<int>::iterator i = input_runs.begin();
		input_runs.erase(input_runs.begin(),input_runs.end());
		cout << "\n----------" << endl;
		for (int j = 0,k = 0; j < num_output_runs(i); ++j, k += input_runs_per_output_run)
		{
				int start = k;
				int end = k + input_runs_per_output_run;
				if(end > output_runs.size()){
					//cout << "0000000" << endl;
					end = output_runs.size();
				}
				cout << "Batch " << j << "	Input Runs:" <<endl;
				print_range_vector(output_runs,start,end);
				input_runs.push_back(merge_k_sorted3(output_runs,start,end,i));
				cout << "Output Run:" << endl;
				print_vector(input_runs[j]);
				cout << "\n" << endl;
				
			
		}
		cout << "----------\n" << endl;

		cout << "Output Runs for Pass "<< i  << " Total Output Runs For This Pass:" << input_runs.size()<< endl << endl;	
		print_2d_array(input_runs);
		cout << "-------------------------------------------\n\n" << endl;

	}

}

int main(int argc, char const *argv[])
{
	
	if (argc != 5)
	{
		cout << "Nead More Arguments" << endl;
		return 0;
	}
	
	if(checkint(argv[2]) != 1){
		printf("Check Command Line Arguments\n");
		return 0;
	}
	if(checkint(argv[3]) != 1){
		printf("Check Command Line Arguments\n");
		return 0;
	}
	
	//int deleteopt;
	if(checkint(argv[4]) != 1){
		printf("Check Command Line Arguments\n");
		return 0;
	}

	buffer_pages = atoi(argv[2]);
	keys_per_page = atoi(argv[3]);
	buffer_per_io = atoi(argv[4]);

	
	FILE *input = fopen(argv[1],"r");
	if (input == NULL)
	{
		cout << "Invalid File name" << endl;
		return 0;
	}


	/*buffer_pages = 6;
	keys_per_page = 2;
	buffer_per_io = 2;
	total_keys = 17;*/


	fscanf(input,"%d",&total_keys);
	vector<int> key;
	int temp;
	while(fscanf(input,"%d",&temp) == 1){
		key.push_back(temp);
	}
	//cout << total_keys << endl;
	int pass0_input_runs = input_runs_pass0();
	//cout << pass0_input_runs << endl;
	for (int i = 0,k = 0; i < pass0_input_runs; ++i,k++)
	{	
		vector<int> temp;
		
		temp.push_back(key[k]);
		input_runs.push_back(temp);
	}
	
	int num_output_runs_passi ;
	int num_passes = 0;
	int check = 0;
	do
	{	
		check = num_output_runs_passi;
		num_output_runs_passi = num_output_runs(num_passes);
		if (check == num_output_runs_passi)
		{
			cout << "Choose different values for given perameter" << endl;
			return 0;
		}
		num_passes++;
	} while (num_output_runs_passi != 1);
		

	for (int i = 0; i < num_passes; ++i)
	{

		// Use passicopy(i) instead of passi(i) if want to see output at each stage in detail
		passi(i);
	}

	return 0;
}