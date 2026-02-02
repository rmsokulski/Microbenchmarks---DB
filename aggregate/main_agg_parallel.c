#include "../header.h"
#include <immintrin.h>
#include <math.h>

/*Definir as estruturas de hash*/

typedef struct {
    int32_t *custkey;
    float *price;
} Hash;
__m512i mulhi_epu32(__m512i a, __m512i b) {
    __m512i evens = _mm512_mul_epu32(a,b);
    __m512i odds = _mm512_mul_epu32(_mm512_srli_epi64(a,32), _mm512_srli_epi64(b,32));
    return _mm512_mask_shuffle_epi32(odds, 0x5555, evens, _MM_SHUFFLE(3,3,1,1)); 
}

__m512i hash_calc(__m512i keys, __m512i mask_buckets, __m512i mask_factor, __m512i off, int hash_size) {

    __m512i hash = _mm512_mullo_epi32(keys, mask_factor);
    hash = mulhi_epu32(hash, mask_buckets);
    hash = _mm512_add_epi64(hash, off);
   return hash;
}

void print_m512i(__m512i x) {
    int32_t values[16];
    _mm512_storeu_si512((__m512i *)values, x);
    for (int i = 0; i < 16; i++) {
        printf("%d ", values[i]);
    }
}

void print_m512(__m512 x) {
    float values[16];
    _mm512_storeu_ps(values, x);
    for (int i = 0; i < 16; i++) {
        printf("%f ", values[i]);
    }
}

/*void build_avx512(Table orders_data, Hash *hash_table, long int hash_size, int orders_size) {
    __m512i key, hash, tab, existing_key;
    __m512 pay;
    __m512i lane_indices = _mm512_set_epi32(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    __m512i empty_key = _mm512_set1_epi32(-1);
    __m512i mask_factor = _mm512_set1_epi32(0x9e3779b9);
    __m512i mask_buckets = _mm512_set1_epi32((hash_size) - 1);
    __m512i mask_empty = _mm512_set1_epi32(-1);
    __m512i mask_unique = _mm512_set1_epi32(-1);
    __m512i mask_1 = _mm512_set1_epi32(1);

    __m512i mask_0 = _mm512_set1_epi32(0);
    __mmask16 m = ~0;
    __m512i off;
   omp_set_num_threads(20);
	#pragma omp parallel private(key, hash, existing_key, pay, off, m) shared(orders_data, hash_table, hash_size, orders_size)
    {
        int tid = omp_get_thread_num();
        int num_threads = omp_get_num_threads();
        int chunk_size = (orders_size + num_threads - 1) / num_threads;
        int start = tid * chunk_size;
        int end = (tid + 1) * chunk_size;
        end = (end > orders_size) ? orders_size : end;
        off = _mm512_xor_epi32(off, off);
        m = ~0;

        for (int32_t input_index = start; input_index + 16 <= end;) {
	        key = _mm512_loadu_epi32(&orders_data.custkeys[input_index]);
	        pay = _mm512_loadu_ps(&orders_data.prices[input_index]);
	        input_index += _mm_popcnt_u32((_mm512_mask2int(m)));

	        // Calculate the probe index using the hash and the current offset
	        hash = hash_calc(key, mask_buckets,mask_factor, off, hash_size);
	        existing_key = _mm512_i32gather_epi32(hash, hash_table->custkey, 4);

	        // Update the existing entries in the hash table
	        __mmask16 existing_mask = _mm512_cmpeq_epi32_mask(existing_key, key);
   	        __m512 existing_pay = _mm512_mask_i32gather_ps(_mm512_setzero_ps(), existing_mask, hash, hash_table->price, 4);
	        existing_pay = _mm512_add_ps(existing_pay, pay);
	        _mm512_mask_i32scatter_ps(hash_table->price, existing_mask, hash, existing_pay, 4);

	        // Insert new entries into the hash table
	        __mmask16 empty_mask = _mm512_cmpeq_epi32_mask(existing_key, mask_empty);
	            _mm512_mask_i32scatter_epi32(hash_table->custkey, empty_mask, hash, key, 4);
	            _mm512_mask_i32scatter_ps(hash_table->price, empty_mask,hash, pay, 4);
	        // Update the mask of processed keys
      	            m = _mm512_kor(existing_mask, empty_mask);

	           // Increase the linear probing offset
		   off = _mm512_add_epi32(off, mask_1);
	           off = _mm512_mask_xor_epi32(off, m, off, off);

		}
	}
}*/

void build_avx512(Table orders_data, Hash *hash_table, long int hash_size, int orders_size) {
    __m512i key, hash, existing_key;
    __m512 pay;
    __m512i lane_indices = _mm512_set_epi32(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    __m512i empty_key = _mm512_set1_epi32(-1);
    __m512i mask_factor = _mm512_set1_epi32(0x9e3779b9);
    __m512i mask_buckets = _mm512_set1_epi32((hash_size) - 1);
    __m512i mask_empty = _mm512_set1_epi32(-1);
    __m512i mask_1 = _mm512_set1_epi32(1);
    __mmask16 m = ~0;
    __m512i mask_0 = _mm512_set1_epi32(0), off;

    #pragma omp parallel for private(key, hash, existing_key, pay) shared(orders_data, hash_table, hash_size, orders_size)
    for (int input_index = 0; input_index < orders_size; input_index+=16) {
        key = _mm512_loadu_epi32(&orders_data.custkeys[input_index]);
        pay = _mm512_loadu_ps(&orders_data.prices[input_index]);
	off = _mm512_xor_epi32(off, off);
        hash = hash_calc(key, mask_buckets, mask_factor, off, hash_size);
        existing_key = _mm512_i32gather_epi32(hash, hash_table->custkey, 4);

        __mmask16 existing_mask = _mm512_cmpeq_epi32_mask(existing_key, key);
        __m512 existing_pay = _mm512_mask_i32gather_ps(_mm512_setzero_ps(), existing_mask, hash, hash_table->price, 4);
        existing_pay = _mm512_add_ps(existing_pay, pay);
	#pragma omp critical
	{
        	_mm512_mask_i32scatter_ps(hash_table->price, existing_mask, hash, existing_pay, 4);
	}
        __mmask16 empty_mask = _mm512_cmpeq_epi32_mask(existing_key, mask_empty);
	#pragma omp critical
	{
        	_mm512_mask_i32scatter_epi32(hash_table->custkey, empty_mask, hash, key, 4);
        	_mm512_mask_i32scatter_ps(hash_table->price, empty_mask, hash, pay, 4);
	}
        m = _mm512_kor(existing_mask, empty_mask);
        off = _mm512_add_epi32(off, mask_1);
        off = _mm512_mask_xor_epi32(off, m, off, off);
    }
}
void aggregate(Table orders, int num_orders, long int hash_size){


     Hash hash_table;
    FILE *output_file;
    char  buffer[25],file_name[12] = "tempos_agg";
    clock_t ticks[2], totalticks[2];
    double elapsedtime, time_register;
    hash_table.custkey = (int *)calloc(hash_size, sizeof(int));
    hash_table.price = (float *)calloc(hash_size, sizeof(float));
    for (int i = 0; i < hash_size; i++) {
        hash_table.custkey[i] = -1; // Inicialize as chaves com -1 (vazio)
    }
    ticks[0] = clock();
    // Chame a função build_avx512 para construir a tabela hash
    build_avx512(orders, &hash_table, hash_size, num_orders);
    ticks[1] = clock();
    elapsedtime = (ticks[1] - ticks[0]) * 1000.0 / CLOCKS_PER_SEC;
    time_register = elapsedtime;
    //Usa a tabela hash (hash_table) para outras operações
     printf("Tabela hash:\n");
    printf("Chave\tValor\n");
    for (long int i = 0; i < hash_size; i++) {
        if (hash_table.custkey[i] != -1) { // Verifique se a posição da tabela hash não está vazia
            printf("Key: %d, Price: %f\n", hash_table.custkey[i], hash_table.price[i]);
        }
    }

    // liberar a memória alocada para as estruturas Table e Hash
    free(hash_table.custkey);
    free(hash_table.price);
    output_file = fopen(file_name,"a");                                                             // opens a file named "tempos" for writing
    fputs("BUILD CPU", output_file);                                                                       // query
    fputs(";", output_file);

    sprintf(buffer, "%f", time_register);                           // puts the time in the buffer string
    fputs(buffer, output_file);
    fputs(";", output_file);
    fclose(output_file);



}

/*alocar espaços*/
Table allocate_table(int num_tuples) {
    Table table;

    // Alocar memória para as colunas numéricas
    table.orderkeys = aligned_alloc(64, num_tuples * sizeof(int32_t));
    table.custkeys = aligned_alloc(64, num_tuples * sizeof(int32_t));
    table.prices = aligned_alloc(64, num_tuples * sizeof(float));
    table.priorities = aligned_alloc(64, num_tuples * sizeof(int));

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
	long int table_1, tuples = 0, table_2;
	uint32_t i = 0, j = 0, l = 0, size;
    int size_hash;
	FILE *input_tab, *output_file;
	char line[1000], *token = NULL, *result = NULL, *p, buffer[25],file_name[12] = "tempos_agg";
        clock_t ticks[2], totalticks[2];
        double elapsedtime, time_register;
	table_1 = strtol(argv[1], &p, 10);
        table_2 = strtol(argv[3], &p, 10);
	/*Reservando espaço de memória para a tabela: */
	orders = allocate_table(table_1);
	// STORE ORDER TAB
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

	orders_out = allocate_table(table_1);
        size_hash = (int) table_2 * 2.6;
	ticks[0] = clock();	
	aggregate(orders, table_1, size_hash);
	// free all
	ticks[1] = clock();
        elapsedtime = (ticks[1] - ticks[0]) * 1000.0 / CLOCKS_PER_SEC;
        time_register = elapsedtime;
	free_table(orders,table_1);
	free_table(orders_out,table_1);

	output_file = fopen(file_name,"a");                                                             // opens a file named "tempos" for writing
        fputs(argv[7], output_file);                                                                    // execution ID
        fputs(";", output_file);
        fputs("AGREGAÇÃO CPU", output_file);                                                                       // query
        fputs(";", output_file);
        fputs(argv[1], output_file);                                                                    // hash function type
        fputs(";", output_file); 

        sprintf(buffer, "%g", time_register);                           // puts the time in the buffer string
        fputs(buffer, output_file);
        fputs(";", output_file);
        fputs ("\n", output_file);
        fclose(output_file);

}
