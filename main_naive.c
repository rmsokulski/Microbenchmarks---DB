#include "header.h"
Table allocate_table(int num_tuples);

void scan(long int tuples, Table orders, float threshold, int threads, int* orderkeys, float* prices) {
    // Variáveis locais
    int j = 0, k = 0;

    // Variáveis para manipulação dos dados
    int b;
    int chunk_size;
    
    // Cria um array de ponteiros para buffers privados de cada thread
    int *private_buffers[threads];
    
    // Configura o número de threads para OpenMP
    omp_set_num_threads(threads);
    
    // Início do loop paralelo com OpenMP
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();  // Identifica o ID da thread
        int num_threads = threads;  // Obtém o número total de threads
        
        // Calcula o tamanho da "porção" de tuplas que cada thread irá processar
        int chunk_size = (tuples / num_threads);
        int start = tid * chunk_size;  // Início da porção de tuplas da thread
        int end = (tid == num_threads - 1) ? tuples : (tid + 1) * chunk_size;  // Fim da porção
        
        // Aloca um buffer privado para esta thread
        int *rids_buf = (int *)malloc((chunk_size + 1) * sizeof(int));
        private_buffers[tid] = rids_buf; 
        int local_k = 1;
        
        // Processa os dados em paralelo dentro da porção atribuída a esta thread
        for (long int i = start; i < end; i++) {
            // Verifica a condição de seleção
            if (orders.prices[i] <= threshold) {
                rids_buf[local_k] = i;  // Armazena o índice da tupla que atende à condição
                local_k++;
            }
        }
        
        // Armazena o número de tuplas que atenderam à condição na primeira posição do buffer
        rids_buf[0] = local_k;
    }
    
    // Inicializa um índice para mesclar os resultados finais
    int merge_index = 0;
    // Loop para mesclar os resultados de todas as threads
    for (int t = 0; t < threads; t++) {
        int *t_buffer = private_buffers[t];  // Obtém o buffer privado da thread
        int buffer_size = t_buffer[0];  // Obtém o tamanho do buffer
        
        // Loop para copiar os resultados do buffer para a tabela de saída
        for (int b = 1; b <= buffer_size; b++) {
            int tuple_index = t_buffer[b];  // Obtém o índice da tupla
            orderkeys[merge_index] = orders.orderkeys[tuple_index];
            prices[merge_index] = orders.prices[tuple_index];
            merge_index++;
        }
        
        // Libera a memória do buffer privado
        free(t_buffer);	
    }
}



/*alocar espaços*/
Table allocate_table(int num_tuples) {
    Table table;

    table.orderkeys = (int *) aligned_alloc(64, num_tuples * sizeof(int));
    table.prices = (float *) aligned_alloc(64, num_tuples * sizeof(float));
    table.priorities = (int *) aligned_alloc(64, num_tuples * sizeof(int));
    table.custkeys = (int *) aligned_alloc(64, num_tuples * sizeof(int));
    // Alocar memória para os ponteiros de string
    table.statuses = malloc(num_tuples * sizeof(char*));
    table.dates = malloc(num_tuples * sizeof(char*));
    table.priorities_char = malloc(num_tuples * sizeof(char*));
    table.clerks = malloc(num_tuples * sizeof(char*));
    table.comments = malloc(num_tuples * sizeof(char*));

    // Alocar memória para cada string individual
    for (int i = 0; i < num_tuples; i++) {
        table.statuses[i] = aligned_alloc(64, 2 * sizeof(char));
        table.dates[i] = aligned_alloc(64, 20 * sizeof(char));
        table.priorities_char[i] = aligned_alloc(64, 20 * sizeof(char));
        table.clerks[i] = aligned_alloc(64, 25 * sizeof(char));
        table.comments[i] = aligned_alloc(64, 79 * sizeof(char));
    }

    return table;
}
void free_table(Table table, int tuples) {
    // libera memória dos vetores de uint32_t e float
    free(table.orderkeys);
    free(table.custkeys);
    free(table.prices);
    free(table.priorities);


    free(table.statuses);
    free(table.dates);
    free(table.priorities_char);
    free(table.clerks);
    free(table.comments);
}

int main(int argc, char *argv[])
{
	Table orders_out, orders;
	long int table_1, tuples = 0;
	uint32_t i = 0, j = 0, l = 0, size;
	FILE *input_tab, *output_file;
	char line[1000], *token = NULL, *result = NULL, *p, buffer[35],  file_name[22] = "tempos_otimizado_o";
	clock_t ticks[2], totalticks[2];
	double elapsedtime, time_register;
	table_1 = strtol(argv[1], &p, 10);
	/*Reservando espaço de memória para a tabela: */
	orders = allocate_table(table_1);
        orders_out = allocate_table(table_1);

	input_tab = fopen(argv[2], "r");
	if (!input_tab)
	{
        printf("Erro. Não foi possível ler a segunda tabela.\n");
		abort();
	}
	i = 0;
	while (1)
	{
		token = NULL;
		result = fgets(line, 1000, input_tab);
		if (feof(input_tab))
			break;
		token = strtok(result, "|");
		for (j = 0; j < 9; j++)
		{
			if (j == 0 && i < table_1)
			{
				if (token != NULL)
					orders.orderkeys[i] = (uint32_t)atol(token);
			}
			token = strtok(NULL, "|");
			if (j == 0 && i < table_1)
			{
				if (token != NULL)
					orders.custkeys[i] = (uint32_t)atol(token);
			}
			if (j == 1 && i < table_1)
			{
				if (token != NULL)
					strcpy(orders.statuses[i], token);
			}
			if (j == 2 && i < table_1)
			{
				if (token != NULL)

					orders.prices[i] = atol(token);
			}
			if (j == 3 && i < table_1)
			{
				if (token != NULL)
					strcpy(orders.dates[i], token);
			}
			if (j == 4 && i < table_1)
			{
				if (token != NULL)
					strcpy(orders.priorities_char[i], token);
			}
			if (j == 5 && i < table_1)
			{
				if (token != NULL)
					strcpy(orders.clerks[i], token);
			}
			if (j == 6 && i < table_1)
			{
				if (token != NULL)
					orders.priorities[i] = atoi(token);
			}
			if (j == 7 && i < table_1)
			{
				if (token != NULL)
					strcpy(orders.comments[i], token);
			}
		}
		i++;
	}
	fclose(input_tab);

	printf("select * from orders WHERE o_totalprice > 50000.0 --");
	// Alocar e alinhar o vetor de inteiros com 64 bytes
    	int *orderkeys = (int *)aligned_alloc(64, table_1 * sizeof(int));
    	if (orderkeys == NULL) {
           printf("Erro ao alocar memória para orderkeys\n");
           exit(1);
         }

         // Alocar e alinhar o vetor de floats com 64 bytes
         float *prices = (float *)aligned_alloc(64, table_1 * sizeof(float));
         if (prices == NULL) {
           printf("Erro ao alocar memória para prices\n");
           exit(1);
         }

         // Preencher os vetores com zeros
        for (int i = 0; i < table_1; i++) {
           orderkeys[i] = 0;
           prices[i] = 0.0f;
         }
	ticks[0] = clock();
	// parallel_select_scan(table_1, orderkeys, custkeys, prices, statuses, dates, priorities,clerks, comments,orderkey_out, custkey_out, totalprice_out, orderstatus_out, orderdate_out, orderpriority_out,clerk_out, comment_out);
	scan(table_1, orders,50000.0,20, orderkeys, prices);
	ticks[1] = clock();
	elapsedtime = (ticks[1] - ticks[0]) * 1000.0 / CLOCKS_PER_SEC;
	time_register = elapsedtime;
        for(i=0;i<table_1;i++){
            if(prices[i] != 0)
              printf("Resultado: %f\n",prices[i]);
        }
	// free all
	free_table(orders,table_1);
	free_table(orders_out,table_1);
	
	output_file = fopen(file_name,"a");								// opens a file named "tempos" for writing
	fputs(argv[7], output_file);									// execution ID
	fputs(";", output_file);
	fputs("SELECT CPU", output_file);									// query
	fputs(";", output_file);
	fputs(argv[1], output_file);									// hash function type
	fputs(";", output_file); 

	sprintf(buffer, "%f", time_register); 				// puts the time in the buffer string
	fputs(buffer, output_file);
	fputs(";", output_file);
	fputs ("\n", output_file);
	fclose(output_file);
}



