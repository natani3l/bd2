#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>
#include <time.h>

#define buffer_size 1048
// gcc -o lib_version main.c  -I/usr/include/postgresql -lpq
struct timespec begin, end;
void do_exit(PGconn *conn) {
    fprintf(stderr, "%s\n", PQerrorMessage(conn));
    PQfinish(conn);
    exit(1);
}

void explicitTransition(PGconn *conn, PGresult *res, FILE* fp) {
		clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
		char buffer[buffer_size];
		int row = 0, column = 0, i = 0;
		res = PQexec(conn, "BEGIN");
		printf("BEGIN START\n");
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			fprintf(stderr, "Comando BEGIN deu ruim");
			PQclear(res);
			do_exit(conn);
		}
		while (fgets(buffer, buffer_size, fp)) {
			char str[buffer_size] = "INSERT INTO product (";
			char str2[buffer_size] = ") VALUES (";
			row++; 
			column = 0;
			if (row == 1) 
				continue; 
			char* value = strtok(buffer, ",");
			while (value)  {
				if (column == 0 && strcmp ("null", value) != 0) {
					strcat(str, "product_name");
					strcat(str2, "'");
					strcat(str2, value);
					strcat(str2, "'");
				} 

				if (column == 1 && strcmp ("null", value) != 0) {
					strcat(str, ", brand_name");
					strcat(str2, ", '");
					strcat(str2, value);
					strcat(str2, "'");
				} 

				if (column == 2 && strcmp ("null", value) != 0) {
					strcat(str, ", asin");
					strcat(str2, ", '");
					strcat(str2, value);
					strcat(str2, "'");
				} 

				value = strtok(NULL, ",");
				column++;
			}
			strcat(str, str2);
			strcat(str, ");");
			res = PQexec(conn, str);
			if (PQresultStatus(res) != PGRES_COMMAND_OK) {
				do_exit(conn);
			}
			PQclear(res);
			i++;
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
	int row = 0, column = 0, i = 0;
	clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
	double time_spent;
	printf("START\n");
	while (fgets(buffer, buffer_size, fp)) {
		char str[buffer_size] = "INSERT INTO product (";
		char str2[buffer_size] = ") VALUES (";
		column = 0; 
		row++; 
		if (row == 1) 
			continue; 
		char* value = strtok(buffer, ",");
		while (value)  {
			if (column == 0 && strcmp ("null", value) != 0) {
				strcat(str, "product_name");
				strcat(str2, "'");
				strcat(str2, value);
				strcat(str2, "'");
			} 

			if (column == 1 && strcmp ("null", value) != 0) {
				strcat(str, ", brand_name");
				strcat(str2, ", '");
				strcat(str2, value);
				strcat(str2, "'");
			} 

			if (column == 2 && strcmp ("null", value) != 0) {
				strcat(str, ", asin");
				strcat(str2, ", '");
				strcat(str2, value);
				strcat(str2, "'");
			} 

			value = strtok(NULL, ",");
			column++;
		}
		strcat(str, str2);
		strcat(str, ");");
		res = PQexec(conn, str);
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			do_exit(conn);
		}
		PQclear(res);
		i++;
	} 
	printf("END");
	fclose(fp); 
	clock_gettime(CLOCK_MONOTONIC_RAW, &end);
	printf ("Total time = %f seconds\n", (end.tv_nsec - begin.tv_nsec) / 1000000000.0 + (end.tv_sec  - begin.tv_sec));
}

void rollback_erro(PGconn *conn, PGresult *res) {
	
}

void rollback(PGconn *conn, PGresult *res, FILE* fp) {
	clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
	char buffer[buffer_size];
	int row = 0, column = 0, i = 0;
	res = PQexec(conn, "BEGIN");
	printf("BEGIN START\n");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		printf("ERRO BEGIN : %s", PQerrorMessage(conn));
		PQclear(res);
		do_exit(conn);
	}
	while (fgets(buffer, buffer_size, fp)) {
		char str[buffer_size] = "INSERT INTO product (";
		char str2[buffer_size] = ") VALUES (";
		row++; 
		column = 0;
		if (row == 1) 
			continue; 
		char* value = strtok(buffer, ",");
		while (value)  {
			if (strcmp ("ERRO", value) == 0) {
				printf("FUNÇÃO DE ROLLBACK\n");
				res = PQexec(conn, "ROLLBACK");
				if (PQresultStatus(res) != PGRES_COMMAND_OK) {
					printf("ROLLBACK ERROR %s", PQerrorMessage(conn));
					PQclear(res);
					break;
				}
			}
			
			if (column == 0 && strcmp ("null", value) != 0) {
				strcat(str, "product_name");
				strcat(str2, "'");
				strcat(str2, value);
				strcat(str2, "'");
			} 

			if (column == 1 && strcmp ("null", value) != 0) {
				strcat(str, ", brand_name");
				strcat(str2, ", '");
				strcat(str2, value);
				strcat(str2, "'");
			} 

			if (column == 2 && strcmp ("null", value) != 0) {
				strcat(str, ", asin");
				strcat(str2, ", '");
				strcat(str2, value);
				strcat(str2, "'");
			} 

			value = strtok(NULL, ",");
			column++;
		}
		strcat(str, str2);
		strcat(str, ");");
		res = PQexec(conn, str);
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			do_exit(conn);
		}
		PQclear(res);
		i++;
	} 
		res = PQexec(conn, "COMMIT");
		printf("BEGIN END\n");
		PQclear(res);
		fclose(fp); 
		clock_gettime(CLOCK_MONOTONIC_RAW, &end);
		printf ("Total time = %.2f seconds\n", (end.tv_nsec - begin.tv_nsec) / 1000000000.0 + (end.tv_sec  - begin.tv_sec));
}

int main() {
	PGconn *conn;
	PGresult *res;
	FILE* fp = fopen("data.csv", "r");

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
		// printf("\nSEM TRANSAÇÂO\n");
		// implicitTransition(conn, res, fp);
		printf("ROLLBACK\n");
		rollback(conn, res, fp);
	}

	return 0;
}
