#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>
#include <time.h>

#define buffer_size 1048

// gcc -o lib_version main.c  -I/usr/include/postgresql -lpq

struct timespec begin, end;

void do_exit(PGconn *conn) {
    printf("%s\n", PQerrorMessage(conn));
    PQfinish(conn);
    exit(1);
}

void explicitTransition(PGconn *conn, PGresult *res, FILE* fp) {
		clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
		char buffer[buffer_size];
		int row = 0, i = 0;
		res = PQexec(conn, "BEGIN");
		printf("BEGIN START\n");
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			printf("BEGIN ERROR %s", PQerrorMessage(conn));
			PQclear(res);
			do_exit(conn);
		}

		while (fgets(buffer, buffer_size, fp)) {
			char str[buffer_size] = "INSERT INTO product (product_name) VALUES ('";
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
			}
		} 

		res = PQexec(conn, "COMMIT");
		printf("BEGIN END\n");
		PQclear(res);
		fclose(fp); 
		clock_gettime(CLOCK_MONOTONIC_RAW, &end);
		printf ("Total time = %.2f seconds\n", (end.tv_nsec - begin.tv_nsec) / 1000000000.0 + (end.tv_sec  - begin.tv_sec));
}

void implicitTransition(PGconn *conn, PGresult *res, FILE* fp) {
	char buffer[buffer_size]; 
	int row = 0, i = 0;
	clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
	printf("START\n");
	while (fgets(buffer, buffer_size, fp)) {
		char str[buffer_size] = "INSERT INTO product (product_name) VALUES ('"; 
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
		}
	} 
	printf("END\n");
	fclose(fp); 
	clock_gettime(CLOCK_MONOTONIC_RAW, &end);
	printf ("Total time = %f seconds\n", (end.tv_nsec - begin.tv_nsec) / 1000000000.0 + (end.tv_sec  - begin.tv_sec));
}

void rollback_erro(PGconn *conn, PGresult *res, FILE* fp) {
	char buffer[buffer_size]; 
	int row = 0, i = 0;
	clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
	res = PQexec(conn, "BEGIN");
	printf("BEGIN START FUNCTION ROLLBACK\n");
	while (fgets(buffer, buffer_size, fp)) {
		char str[buffer_size] = "INSERT INTO product (product_name) VALUES ('"; 
		row++; 
		if (row == 1) 
			continue; 

		char* value = strtok(buffer, "\t"); 

		while (value) {
			if (i == 100) {
				strcat(str, value);
				strcat(str, "')]");
			} else {
				strcat(str, value);
				strcat(str, "')");
			}

			res = PQexec(conn, str);

			if (PQresultStatus(res) != PGRES_COMMAND_OK) {
				printf("SOMETHING WRONG WAS HAPPENED... ROLLBACK\n");
				res = PQexec(conn, "ROLLBACK");
				do_exit(conn);
			}
			PQclear(res);
			value = strtok(NULL, ", ");
		}
		i++;
	} 
	printf("END COMMIT FUNCTION ROLLBACK\n");
	res = PQexec(conn, "COMMIT");
	fclose(fp); 
	clock_gettime(CLOCK_MONOTONIC_RAW, &end);
	printf ("Total time = %f seconds\n", (end.tv_nsec - begin.tv_nsec) / 1000000000.0 + (end.tv_sec  - begin.tv_sec));
}

int main() {
	PGconn *conn;
	PGresult *res;
	FILE* fp = fopen("data.csv", "r");

	int lib_ver = PQlibVersion();

	printf("Version: %d\n", lib_ver);

	conn = PQconnectdb("user=postgres password=admin dbname=teste2");

	if (PQstatus(conn) == CONNECTION_BAD) {
			printf("%s", PQerrorMessage(conn));
			do_exit(conn);
	}

	if (!fp) {
		printf("Não foi possível abrir o arquivo CSV");
	} else {
		// printf("COM TRANSAÇÂO\n");
		// explicitTransition(conn, res, fp);
		printf("SEM TRANSAÇÂO\n");
		implicitTransition(conn, res, fp);
		// printf("ROLLBACK\n");
		// rollback_erro(conn, res, fp);
	}

	return 0;
}
