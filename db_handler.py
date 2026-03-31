from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta
import helper_functions


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """

    sk_query = r"SELECT MAX(i_item_sk) FROM item;"
    cur.execute(sk_query)
    max_sk = [row for row in cur]

    item_sk = int(max_sk[0][0]) + 1
    start_date = f"{new_item.start_year}-01-01"
    item_class = "New Item"

    cur.execute(
        """
        INSERT INTO item
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            item_sk,
            new_item.item_id,
            start_date,
            new_item.product_name,
            new_item.brand,
            item_class,
            new_item.category,
            new_item.manufact,
            new_item.current_price,
            new_item.num_owned,
        )
    )



def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """

    sk_query = r"SELECT MAX(ca_address_sk) FROM customer_address;"
    cur.execute(sk_query)
    ca_sk = int([row for row in cur][0][0]) + 1
    address = new_customer.address

    segment = address.find(' ')
    street_number = address[:segment]
    address = address[segment:]

    segment = address.find(',')
    street_name = address[:segment]
    address = address[segment + 1:]

    segment = address.find(',')
    city = address[:segment]
    address = address[segment + 1:]

    segment = address.find(' ')
    state = address[:segment]
    address = address[segment:]
    zip = address

    cur.execute(
        """
        INSERT INTO customer_address
        VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            ca_sk,
            street_number,
            street_name,
            city,
            state,
            zip
        )
    )

    sk_query = r"SELECT MAX(c_customer_sk) FROM customer;"
    cur.execute(sk_query)
    c_sk = int([row for row in cur][0][0]) + 1

    name_space = new_customer.name.find(' ')
    first = new_customer.name[:name_space]
    last = new_customer.name[name_space:]

    cur.execute(
        """
        INSERT INTO customer
        VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            c_sk,
            new_customer.customer_id,
            first,
            last,
            new_customer.email,
            ca_sk
        )
    )


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """

    updates = []
    values = []
    if new_customer.customer_id is not None:
        updates.append("c_customer_id = %s")
        values.append(new_customer.customer_id)
    if new_customer.name is not None:
        name_space = new_customer.name.find(' ')
        first = new_customer.name[:name_space]
        last = new_customer.name[name_space + 1:]

        updates.append("c_first_name = %s")
        updates.append("c_last_name = %s")
        values.append(first)
        values.append(last)
    if new_customer.email is not None:
        updates.append("c_email_address = %s")
        values.append(new_customer.email)
    if new_customer.address is not None:
        cur.execute(
            """
            SELECT ca_address_sk FROM customer_address
            WHERE EXISTS(
            SELECT c_current_addr_sk FROM customer
            WHERE c_current_addr_sk = customer_address.ca_address_sk
            AND c_customer_id = %s);
            );
            """, (
                original_customer_id
            )
        )
        ca_sk = int([row for row in cur][0][0])
        address = new_customer.address

        segment = address.find(' ')
        street_number = address[:segment]
        address = address[segment:]

        segment = address.find(',')
        street_name = address[:segment]
        address = address[segment + 1:]

        segment = address.find(',')
        city = address[:segment]
        address = address[segment + 1:]

        segment = address.find(' ')
        state = address[:segment]
        address = address[segment:]
        zip = address

        cur.execute(
            """
            UPDATE customer_address
            SET ca_street_number = %s, ca_street_name = %s, ca_city = %s, ca_state = %s, ca_zip = %s
            WHERE ca_address_sk = %s;
            """, (
                street_number,
                street_name,
                city,
                state,
                zip,
                ca_sk
            )
        )

    full_update = ", ".join(updates)
    values.append(original_customer_id)

    query = f"""
    UPDATE customer
    SET {full_update}
    WHERE c_customer_id = %s;
    """

    cur.execute(query, values)


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """

    cur.execute(
        """
        INSERT INTO rental
        VALUES (%s, %s, %s, %s);
        """, (
            item_id,
            customer_id,
            date.today(),
            date.today() + timedelta(days = 14)
        )
    )


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """

    cur.execute("SELECT * FROM waitlist WHERE item_id = %s;", (item_id,))
    place = len([row for row in cur]) + 1

    cur.execute(
        """
        INSERT INTO waitlist
        VALUES (%s, %s, %s);
        """, (
            item_id,
            customer_id,
            place
        )
    )

    return place

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """

    cur.execute(
        """
        DELETE FROM waitlist
        WHERE place_in_line = 1;
        """
    )

    cur.execute(
        """
        UPDATE waitlist
        SET place_in_line = place_in_line - 1
        """
    )


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """

    cur.execute("SELECT * FROM rental WHERE item_id = %s AND customer_id = %s;", (item_id, customer_id))
    attributes = [row for row in cur][0]

    cur.execute(
        """
        INSERT INTO rental_history
        VALUES (%s, %s, %s, %s, %s);
        """, (
            attributes[0],
            attributes[1],
            attributes[2],
            date.today(),
            date.today()
        )
    )

    cur.execute(
        """
        DELETE FROM rental
        WHERE item_id = %s AND customer_id = %s;
        """, (
            item_id,
            customer_id
        )
    )


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """

    cur.execute("SELECT due_date FROM rental WHERE item_id = %s AND customer_id = %s;", (item_id, customer_id))
    old_date = [row for row in cur][0][0]
    new_date = old_date + timedelta(days = 14)

    cur.execute(
        """
        UPDATE rental
        SET due_date = %s
        WHERE item_id = %s AND customer_id = %s;
        """, (
            new_date,
            item_id,
            customer_id
        )
    )


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """

    filters = []
    values = []
    if filter_attributes.item_id is not None:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_item_id LIKE %s")
        else:
            filters.append("i_item_id = %s")
        values.append(filter_attributes.item_id)
    if filter_attributes.product_name is not None:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_product_name LIKE %s")
        else:
            filters.append("i_product_name = %s")
        values.append(filter_attributes.product_name)
    if filter_attributes.brand is not None:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_brand LIKE %s")
        else:
            filters.append("i_brand = %s")
        values.append(filter_attributes.brand)
    if filter_attributes.category is not None:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_category LIKE %s")
        else:
            filters.append("i_category = %s")
        values.append(filter_attributes.category)
    if filter_attributes.manufact is not None:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_manufact LIKE %s")
        else:
            filters.append("i_manufact = %s")
        values.append(filter_attributes.manufact)
    if filter_attributes.current_price != -1:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_current_price LIKE %s")
        else:
            filters.append("i_current_price = %s")
        values.append(filter_attributes.current_price)
    if filter_attributes.start_year != -1:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_rec_start_date LIKE %s")
        else:
            filters.append("i_rec_start_date = %s")
        values.append(f"{filter_attributes.start_year}-1-1")
    if filter_attributes.num_owned != -1:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("i_num_owned LIKE %s")
        else:
            filters.append("i_num_owned = %s")
        values.append(filter_attributes.num_owned)

    if min_price != -1:
        if len(filters) != 0:
            filters.append(" AND ")
        filters.append("i_current_price >= %s")
        values.append(min_price)
    if max_price != -1:
        if len(filters) != 0:
            filters.append(" AND ")
        filters.append("i_current_price <= %s")
        values.append(max_price)
    if min_start_year != -1:
        if len(filters) != 0:
            filters.append(" AND ")
        filters.append("i_start_year >= %s")
        values.append(min_start_year)
    if max_start_year != -1:
        if len(filters) != 0:
            filters.append(" AND ")
        filters.append("i_start_year <= %s")
        values.append(max_start_year)

    full_filter = "".join(filters)
    query = f"""
        SELECT * FROM item
        WHERE {full_filter};
        """
    cur.execute(query, values)

    results = []
    for row in cur:
        results.append(Item(row[1], row[3], row[4], row[6], row[7], row[8], row[2], row[9]))

    return results


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    filters = []
    values = []
    if filter_attributes.customer_id is not None:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("c_customer_id LIKE %s")
        else:
            filters.append("c_customer_id = %s")
        values.append(filter_attributes.customer_id)
    if filter_attributes.name is not None:
        name_space = filter_attributes.name.find(' ')
        first = filter_attributes.name[:name_space]
        last = filter_attributes.name[name_space + 1:]

        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("c_first_name LIKE %s")
            filters.append(" AND ")
            filters.append("c_last_name LIKE %s")
        else:
            filters.append("c_first_name = %s")
            filters.append(" AND ")
            filters.append("c_last_name = %s")
        values.append(first)
        values.append(last)
    if filter_attributes.address is not None:
        address = filter_attributes.address

        segment = address.find(' ')
        street_number = address[:segment]
        address = address[segment:]

        segment = address.find(',')
        street_name = address[:segment]
        address = address[segment + 1:]

        segment = address.find(',')
        city = address[:segment]
        address = address[segment + 1:]

        segment = address.find(' ')
        state = address[:segment]
        address = address[segment:]
        zip = address

        cur.execute(
            """
            SELECT c_current_addr_sk FROM customer
            WHERE EXISTS(
            SELECT ca_address_sk FROM customer_address
            WHERE ca_address_sk = customer.c_current_addr_sk
            AND ca_street_number = %s
            AND ca_street_name = %s
            AND ca_city = %s
            AND ca_state = %s
            AND ca_zip = %s
            );
            """, (
                street_number,
                street_name,
                city,
                state,
                zip
            )
        )
        c_sk = int([row for row in cur][0][0])

        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("c_customer_sk LIKE %s")
        else:
            filters.append("c_customer_sk = %s")
        values.append(c_sk)
    if filter_attributes.email is not None:
        if len(filters) != 0:
            filters.append(" AND ")
        if use_patterns:
            filters.append("c_email_address LIKE %s")
        else:
            filters.append("c_email_address = %s")
        values.append(filter_attributes.email)

    full_filter = "".join(filters)
    query = f"""
        SELECT * FROM customer
        WHERE {full_filter};
        """
    cur.execute(query, values)

    results = []
    for row in cur:
        cur.execute("SELECT * FROM customer_address WHERE ca_address_sk = %s", (row[5],))
        address = ""
        for add in cur:
            # street_number street_name, city, state zip
            address = f"{add[1]} {add[2]}, {add[3]}, {add[4]} {add[5]}"

        results.append(Customer(row[1], f"{row[2]} {row[3]}", address, row[4]))

    return results

def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """

    if not helper_functions.check_if_item_exists(item_id):
        return -1

    cur.execute("SELECT i_num_owned FROM item WHERE i_item_id = %s;", (item_id,))
    num_owned = int([row for row in cur][0][0])

    cur.execute("SELECT * FROM rental WHERE item_id = %s", (item_id,))
    num_rented = len([row for row in cur])

    return num_owned - num_rented


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """

    cur.execute("SELECT place_in_line FROM waitlist WHERE item_id = %s AND customer_id = %s;", (item_id, customer_id))

    try:
        return int([row for row in cur][0][0])
    except IndexError:
        return -1


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """

    cur.execute("SELECT MAX(place_in_line) FROM waitlist WHERE item_id = %s;", (item_id,))

    try:
        return int([row for row in cur][0][0])
    except TypeError:
        return 0


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    conn.close()

