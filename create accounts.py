
# Imports the Google Cloud Client Library.
from google.cloud import spanner
import uuid
import random
import decimal
import datetime;
import base64
from proto import INT64;
import os

# Your Cloud Spanner instance ID.
instance_id = "bitfoon-dev"
# Your Cloud Spanner database ID.
database_id = "finance"
# Instantiate a client.
spanner_client = spanner.Client()
# Get a Cloud Spanner instance by ID.
instance = spanner_client.instance(instance_id)
# Get a Cloud Spanner database by ID.
database = instance.database(database_id)
 
customers = 0
roles = 0
accounts = 0
transactions = 0
created_account_list= []

Customer_Type = spanner.param_types.Struct(
    [
        spanner.param_types.StructField("CustomerId", spanner.param_types.BYTES),
        spanner.param_types.StructField("Address", spanner.param_types.STRING),
        spanner.param_types.StructField("Name", spanner.param_types.STRING)
    ]
)
insert_customer_stmt ="INSERT Customer (CustomerId, Address, Name) VALUES (@p.CustomerId, @p.Address, @p.Name)" 

Role_Type = spanner.param_types.Struct(
    [
        spanner.param_types.StructField("CustomerId", spanner.param_types.BYTES),
        spanner.param_types.StructField("RoleID", spanner.param_types.BYTES),
        spanner.param_types.StructField("AccountId", spanner.param_types.BYTES),
        spanner.param_types.StructField("Role", spanner.param_types.STRING)
    ]
)
insert_role_stmt ="INSERT CustomerRole (CustomerId, RoleId, AccountId, Role) VALUES (@p.CustomerId, @p.RoleId, @p.AccountId, @p.Role)" 

Account_Type = spanner.param_types.Struct(
    [
        spanner.param_types.StructField("AccountId", spanner.param_types.BYTES),
        spanner.param_types.StructField("AccountStatus", spanner.param_types.INT64),
        spanner.param_types.StructField("Balance", spanner.param_types.NUMERIC),
        spanner.param_types.StructField("CreationTimestamp", spanner.param_types.TIMESTAMP)
    ]
)
insert_account_stmt ="INSERT Account (AccountId, AccountStatus, Balance, CreationTimeStamp) VALUES (@p.AccountId, @p.AccountStatus, @p.Balance, @p.CreationTimeStamp)" 

Transaction_Type = spanner.param_types.Struct(
    [
        spanner.param_types.StructField("AccountId", spanner.param_types.BYTES),
        spanner.param_types.StructField("EventTimestamp", spanner.param_types.TIMESTAMP),
        spanner.param_types.StructField("Amount", spanner.param_types.NUMERIC),
        spanner.param_types.StructField("Description", spanner.param_types.STRING),
        spanner.param_types.StructField("IsCredit", spanner.param_types.BOOL)
    ]
)
insert_transaction_stmt ="INSERT TransactionHistory (AccountId, EventTimestamp, Amount, Description, IsCredit) VALUES (@p.AccountId, @p.EventTimestamp, @p.Amount, @p.Description, @p.IsCredit)" 


#used to insert in DB
def db_insert(transaction, insert_stmt, value, type):
    
    row_ct = transaction.execute_update(
            insert_stmt,
            params={'p':value},
            param_types={"p": type}
    )
    return row_ct


#Updater for the UI
def range_with_status(total):
    """ iterate from 0 to total and show progress in console """
    n=0
    while n<total:
        done = '#'*(n+1)
        todo = '-'*(total-n-1)
        s = '<{0}>'.format(done+todo)
        if not todo:
            s+='\n'        
        if n>0:
            s = '\r'+s
        print(s, end='')
        yield n
        n+=1



def insert_customer(transaction):
    CustomerId = base64.b64encode(uuid.uuid4().bytes)
    Address = str(base64.b64encode(random.randbytes(random.randint(1,150))))
    Name = str(base64.b64encode(random.randbytes(random.randint(1,20)))) + " " + str(base64.b64encode(random.randbytes(random.randint(1,20))))
    #create a random number of accounts for the customer between 1 and 6
    db_insert(transaction,insert_customer_stmt, (CustomerId, Address, Name),Customer_Type)
    global customers
    customers = customers +1
    #add some ramdom accounts to the customer    
    Account_Count= random.randint(1,3)
    a = 0
    created_account_list.clear()
    while a<Account_Count:
        accountid = insert_account(transaction)
        created_account_list.append(accountid)
        a=a+1
 
    return CustomerId

def insert_role(transaction, CustomerId, AccountId):

    RoleId = base64.b64encode(uuid.uuid4().bytes)
    Role = random.choice(["owner", "viewer", "custodian", "user", "creditor"])

    db_insert(transaction, insert_role_stmt, (CustomerId, RoleId, AccountId, Role),Role_Type)
    global roles
    roles = roles +1


def insert_account(transaction):
     #account id for bank account
    AccountId = base64.b64encode(uuid.uuid4().bytes)
    #set to 0 or 1
    AccountStatus= random.randint(0,1)
    # random decimal
    Balance = round(decimal.Decimal(str(random.uniform(1, 20000))),2)
    #now for the insertion timestamp 
    CreationTimestamp = datetime.datetime.utcnow().isoformat() + "Z"
     #create a random number of accounts for the customer between 1 and 6
    db_insert(transaction,insert_account_stmt, (AccountId, AccountStatus, Balance,CreationTimestamp),Account_Type)
    global accounts
    accounts = accounts+1
    Trans_Count = random.randint(0,25);
    t = 0
    while t < Trans_Count:
        insert_transaction(transaction,AccountId)
        t=t+1
    return AccountId
   
def insert_transaction(transaction,AccountId):
    #set to 0 or 1
    EventTimestamp = datetime.datetime.utcnow().isoformat() + "Z"
    # random decimal
    Amount = round(decimal.Decimal(str(random.uniform(1, 20000))),2)
     #Description for bank account
    Description = str(base64.b64encode(random.randbytes(random.randint(1,500))))
    #set to 0 or 1
    IsCredit= bool(random.randint(0,1))
    db_insert(transaction,insert_transaction_stmt,(AccountId, EventTimestamp, Amount, Description, IsCredit),Transaction_Type)
    global transactions
    transactions=transactions+1

########################## Main #################
count = 0
os.system('clear')
print("Inserting 2000 Customers with accounts, roles, and transactions.... ")
for count in range_with_status(200):
    l=0
    while l < 10:
        cu_id = database.run_in_transaction(insert_customer)
        #add some random roles to the customer_account
        for acct in created_account_list:
            Role_Count= random.randint(1,3)
            r = 0
            while r<Role_Count:
                database.run_in_transaction(insert_role,cu_id,acct)
                r=r+1
        l=l+1    
    count = count + 1

print("Finished. Customers: {} Roles: {} Accounts: {} Transacations: {}".format(customers, roles, accounts, transactions))

