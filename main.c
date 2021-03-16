#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>


// gcc -o lib_version main.c  -I/usr/include/postgresql -lpq

void do_exit(PGconn *conn/*, PGresult *res*/ ) {
    
    fprintf(stderr, "%s\n", PQerrorMessage(conn));    

    // PQclear(res);
    PQfinish(conn);    
    
    exit(1);
}

int main() {
	PGconn *conn;
	PGresult *res;
	clock_t begin, end;
	double time_spent;

	FILE* fp = fopen("data_2.csv", "r"); 
	conn = PQconnectdb("user=postgres password=admin dbname=teste2");

	if (PQstatus(conn) == CONNECTION_BAD) {
			fprintf(stderr, "%s", PQerrorMessage(conn));
			do_exit(conn);
	}

	if (!fp) {
		printf("Não foi possível abrir o arquivo CSV");
	} else {
		char buffer[15791];
		int row = 0;
		int column = 0;

		res = PQexec(conn, "BEGIN");
		printf("BEGIN START\n");
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			fprintf(stderr, "Comando BEGIN deu ruim");
			PQclear(res);
			do_exit(conn);
		}

		begin = clock();

		while (fgets(buffer, 15791, fp)) {
			char str[15791] = "INSERT INTO product (product_name) VALUES ('";
			column = 0; 
			row++; 
			if (row == 1) 
				continue; 

			char* value = strtok(buffer, "\t"); 

			while (value) {
				strcat(str, value);
				strcat(str, "')");
				res = PQexec(conn, str);


				if (PQresultStatus(res) != PGRES_COMMAND_OK) {
					do_exit(conn);
				}
				PQclear(res);
				value = strtok(NULL, ", "); 
				column++;
			}
		} 

		res = PQexec(conn, "END");
		printf("BEGIN END\n");
		PQclear(res);
		fclose(fp); 
		end = clock();
		time_spent = (double)(end-begin)/(CLOCKS_PER_SEC/1000);
		printf("Tempo (em milissegundos): %lf", time_spent); 
		time_spent = (double)(end-begin)/CLOCKS_PER_SEC;
		printf("\nTempo (em segundos): %lf \n", time_spent);
	}


	// ------------------------------- //


	printf("\n\n\n SEM TRANSAÇÂO\n");
	char buffer[25361]; 
	int row = 0;
	int column = 0;

	begin = clock();

	while (fgets(buffer, 25361, fp)) {
		char str[25361] = "INSERT INTO product (product_name) VALUES ('";
		column = 0; 
		row++; 
		if (row == 1) 
			continue; 

		char* value = strtok(buffer, "\t"); 

		while (value) {
			strcat(str, value);
			strcat(str, "')");
			res = PQexec(conn, str);

			if (PQresultStatus(res) != PGRES_COMMAND_OK) {
				do_exit(conn);
			}
			PQclear(res);
			value = strtok(NULL, ", "); 
			column++;
		}
	} 

	fclose(fp); 
	end = clock();
	time_spent = (double)(end-begin)/(CLOCKS_PER_SEC/1000);  //CLOCKS_PER_SEC foi dividido por 1000, para apresentar o tempo em milissegundos.
	printf("Tempo (em milissegundos): %lf", time_spent); 
	time_spent = (double)(end-begin)/CLOCKS_PER_SEC;  //CLOCKS_PER_SEC foi dividido por 1000, para apresentar o tempo em milissegundos.
	printf("\nTempo (em segundos): %lf \n", time_spent);

	return 0;
}


