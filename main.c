#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#define buffer_size 1048
// gcc -o lib_version main.c  -I/usr/include/postgresql -lpq

void do_exit(PGconn *conn) {
    fprintf(stderr, "%s\n", PQerrorMessage(conn));
    PQfinish(conn);
    exit(1);
}

void explicitTransition(PGconn *conn, PGresult *res, FILE* fp) {
		char buffer[buffer_size];
		int row = 0;
		int column = 0;
		clock_t begin, end;
		double time_spent;

		res = PQexec(conn, "BEGIN");
		printf("BEGIN START\n");
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			fprintf(stderr, "Comando BEGIN deu ruim");
			PQclear(res);
			do_exit(conn);
		}

		begin = clock();

		while (fgets(buffer, buffer_size, fp)) { //talvez getline()
			char str[buffer_size] = "INSERT INTO product (product_name) VALUES ('";
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
					res = PQexec(conn, "ROLLBACK");
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

void implicitTransition(PGconn *conn, PGresult *res, FILE* fp) {
	char buffer[buffer_size]; 
	int row = 0;
	int column = 0;
	clock_t begin, end;
	double time_spent;

	begin = clock();
	printf("START\n");
	while (fgets(buffer, buffer_size, fp)) {
		char str[buffer_size] = "INSERT INTO product (product_name) VALUES ('";
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
	printf("END");
	fclose(fp); 
	end = clock();
	time_spent = (double)(end-begin)/(CLOCKS_PER_SEC/1000);
	printf("Tempo (em milissegundos): %lf", time_spent); 
	time_spent = (double)(end-begin)/CLOCKS_PER_SEC;
	printf("\nTempo (em segundos): %lf \n", time_spent);
}


int main() {
	PGconn *conn;
	PGresult *res;


	FILE* fp = fopen("data.csv", "r");

	conn = PQconnectdb("user=postgres password=admin dbname=teste2");

	if (PQstatus(conn) == CONNECTION_BAD) {
			fprintf(stderr, "%s", PQerrorMessage(conn));
			do_exit(conn);
	}

	if (!fp) {
		printf("Não foi possível abrir o arquivo CSV");
	} else {
		printf("COM TRANSAÇÂO\n");
		explicitTransition(conn, res, fp);
		printf("\n\n\nSEM TRANSAÇÂO\n");
		implicitTransition(conn, res, fp);
	}


	


	return 0;
}
