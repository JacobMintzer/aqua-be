import psycopg2
import psycopg2.extras

FUZZY_SEARCH = """
                SELECT
                address_city,
                address_state,
                address_street,
                address_zip,
                first_name,
                last_name,
                ssn,
                age
                FROM public.address
                JOIN public.person
                ON person.personid = address.personid
                WHERE (
                address.address_city ilike %(query)s OR
                address.address_state ilike %(query)s OR
                address.address_street ilike %(query)s OR
                address.address_zip ilike %(query)s OR
                person.first_name ilike %(query)s OR
                person.last_name ilike %(query)s OR
                person.ssn = %(queryNum)s OR
                person.age = %(queryNum)s
                )
                ORDER BY person.personid
                OFFSET %(offset)s
                """
EXACT_SEARCH = """
                SELECT
                address_city,
                address_state,
                address_street,
                address_zip,
                first_name,
                last_name,
                ssn,
                age
                FROM public.address
                JOIN public.person
                ON person.personid = address.personid
                WHERE (
                address.address_city ilike %(query)s OR
                address.address_state ilike %(query)s OR
                address.address_street ilike %(query)s OR
                address.address_zip ilike %(query)s OR
                person.first_name ilike %(query)s OR
                person.last_name ilike %(query)s OR
                person.ssn = %(queryNum)s OR
                person.age = %(queryNum)s
                )
                ORDER BY person.personid
                OFFSET %(offset)s
                """


class Client:
    def __init__(self, credentials):
        self.conn = psycopg2.connect(
            database=credentials["DbName"],
            user=credentials["Username"],
            password=credentials["Password"],
            host=credentials["ConnectionUrl"],
            port=credentials["Port"],
        )
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def search(self, query: str, page: int = 0, exact=False):
        offset = 30 * page
        if not query:
            self.cur.execute(
                """
                SELECT
                address_city,
                address_state,
                address_street,
                address_zip,
                first_name,
                last_name,
                ssn,
                age
                FROM public.address
                JOIN public.person
                ON person.personid = address.personid
                ORDER BY person.personid
                OFFSET %(offset)s
                """,
                {"offset": offset},
            )
        else:
            if query.isnumeric():
                queryNum = int(query)
            else:
                queryNum = None
            if exact:
                self.cur.execute(
                    EXACT_SEARCH,
                    {"offset": offset, "query": query, "queryNum": queryNum},
                )
            else:
                fuzzy_query = f"%{query}%"
                self.cur.execute(
                    FUZZY_SEARCH,
                    {"offset": offset, "query": fuzzy_query, "queryNum": queryNum},
                )
        return self.cur.rowcount, self.cur.fetchmany(30)
