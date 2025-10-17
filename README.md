# EventfulDB

Owner: Patrick Thach
Tags: Infrastructure
Author: Patrick Thach
Port: 7070
Status: In progress

## Overview

EventfulDB is an event-sourced, key-value, write-side database system designed to provide immutable, append-only storage for events across various domains. It is ideal for applications requiring detailed audit trails for compliance, complex business processes involving states, and high data integrity levels.

## Background

My drive to create this immutable system was inspired by three key scenarios I've encountered:

- First, during my years in the healthcare industry, which involved handling sensitive patient records, I faced situations where patient information was either out of sync or had been altered/tampered with without any trace of what or who caused those changes. This could be due to system errors or user actions. I envisioned a system where patient data are immutable, ensuring that if alterations occur, there would be a way to trace what or who made these changes. There needs to be a single source of truth database system into which everyone/every system can write given the proper permissions, and where administrators cannot simply alter data without it being logged.
- Second, as a proponent of the CQRS pattern, I appreciate the separation between write and read databases. I've dealt with a replication task that involved defining numerous aggregates, loading history for each aggregate instance, creating snapshots, and managing versioning and side effects. There should be a system to handle these underlying steps, potentially saving 20% or more of production code. There are needs for a system to handle the write-side of CQRS well.
- Third, I believe a database system should excel at managing side effects. For instance, if there's a change in a patient's state, the system should notify or potentially modify the state in other systems (through integration) to mirror this change, possibly using field-by-field mapping. For example, a **`patient-updated`** event indicating a change in a patient's state should seamlessly update other systems. Specifically, changing a patient's status to deceased should automatically update the indices in OpenSearch to reflect the deceased status, employing field-by-field mapping. These side effects will be efficiently managed by plugins, enabling system administrators to enable, disable, add, or remove side effects as necessary. It should be as straightforward as mapping the fields in event X to the fields in System X and updating System X fields accordingly.

## Features

- **String-Only Storage**: EventfulDB adopts a string-only storage format, meaning all data, regardless of its original type (e.g., integers, dates), is stored as strings. This approach simplifies data handling and ensures uniformity across the database, which is particularly beneficial for data integrity and auditability.
- **Immutable Data Structure**: Once data is entered into EventfulDB, it becomes immutable, meaning it cannot be altered or deleted. This characteristic is crucial for applications where the accuracy and traceability of historical data are paramount, such as medical records, financial transactions, and supply chain management. Data can be archived, moving from short-term to long-term storage, but cannot be deleted.
- **Event Sourcing and Replay**: EventfulDB is built on the principle of event sourcing, storing all changes to the data as a sequence of events. This allows for the complete replay of events to reconstruct the database's state at any point in time, thereby enhancing data recovery and audit capabilities. Unlike traditional databases that execute update statements to modify data, this system is event-driven. Aggregate state changes are defined in the event object, allowing these events to be replayed at any time to reconstruct the aggregate's current state.
- **Merkle Tree Integration**: Each aggregate in EventualDB is associated with a Merkle tree of events, enabling verification of data integrity. The Merkle tree structure ensures that any data tampering can be detected, offering an additional security layer against data corruption.
- **Built-in Audit Trails**: EventfulDB automatically maintains a comprehensive audit trail of all transactions, a feature invaluable for meeting compliance and regulatory requirements. It provides transparent and tamper-evident records. During audits, administrators can issue specific tokens to auditors to access and review specific aggregate instances and all relevant events associated with those instances.
- **Security with Token-Based Authorization**: EventfulDB implements token-based authorization to manage database access. This approach allows for precise control over who can access and modify data, protecting against unauthorized changes. Unlike systems where a single application user account performs CRUD operations, EventualDB mandates that each user of the application obtains their own access token with a specific time horizon. For example, a doctor will receive their own access token, linked to their identifier (IAM-managed outside EventfulDB), generated by the system for each event they handle, rather than having a single application user manage all CRUD operations. This ensures the system can accurately track that it was the doctor who made changes to an aggregate state, not the application.
- **Powered by RocksDB and Rust**: At its core, EventualDB utilizes RocksDB for storage, taking advantage of its high performance and efficiency. The system is developed in Rust, known for its safety, efficiency, and concurrency capabilities, ensuring that EventualDB is both rapid and dependable.

<aside>
💡 EventfulDB is optimally employed within a microservice architecture, wherein each microservice is aligned with a distinct domain context. For example, at OM.Farm, distinct microservices are deployed for Farm Management, Livestock Management, and Livestock Leasing Agreements. Similarly, in the health sector, there could be separate microservices for managing patient demographics and patient appointments. Each microservice operates its own EventfulDB instance, tailored for its specialized purposes.

</aside>

## **System Commands**

- **`config`**: Setup the EventfulDB system, preparing it for first-time use by setting up necessary configurations or update the system configuration.
  ```toml
  # Configure Eventful with a Master Key (only for initial setup)
  # Limit the number of events in memory to 10,000
  # using a FIFO (First-In, First-Out) queue approach.
  # Set the Data Encryption Key for data at rest.

  eventful config --master-key=xxx --memory-threshold=10000 --dek=xxxx-xxx-xxx
  ```
- You must supply both `--master-key` and `--dek` the first time you configure EventfulDB; the CLI will refuse to start the server or issue tokens until both secrets are present. The master key governs token derivation, while the `--dek` value enables transparent data encryption (TDE-style) so persisted storage cannot be read without the configured key.
- By default, EventfulDB stores its data under `./.eventful` inside the application directory (with configuration persisted at `./.eventful/config.toml`). You can point `--data-dir` elsewhere if you prefer a custom location.
- **`start`**: Launches the EventualDB server, making it ready to accept and process requests.
  ```toml
  # start eventful service on port 9595 (default)
  eventful start --port 9595
  ```
- **`stop`**: Gracefully shuts down the EventfulDB server, ensuring that all processes are correctly terminated.
  ```toml
  # stop eventful service gracefully (end all connections and services)
  eventful stop --all

  # stop individual services
  eventful stop --graphql --restful --grpc

  # stop eventful service immediate, ending all services
  evenful stop --force
  ```
- **`restart`**: Stops the running EventfulDB server (if any) and launches a fresh instance, ensuring schemas and tokens are reloaded from disk.
  ```toml
  # restart the daemonized server
  eventful restart

  # restart and run in the foreground
  eventful restart --foreground
  ```
- **`destroy`**: Removes all EventfulDB data, configuration, and metadata, returning the environment to a pristine, first-install state.
  ```toml
  # destroy all EventfulDB state (prompts for confirmation)
  eventful destroy

  # destroy without prompt
  eventful destroy --yes
  ```
- **`version`**: Displays the current version of the EventfulDB system, helping users identify the installed version.
  ```toml
  # display the current version
  eventful --version
  ```
- **`upgrade`**: Upgrades EventfulDB to a newer version, applying new system features and patches etc. EventfulDB must guaranteed that upgrading to newer version would not break existing data.
  ```toml
  # upgrade the eventful to the latest version
  eventful upgrade@latest -f

  # see what will change prior to applying the upgrade
  eventful upgrade@latest --dry-run
  ```
- **`remote`**: Manage EventfulDB nodes. All data will be eventually be synced crossed all nodes
  ```toml
  # add a remote database call upstream (can be anyname)
  eventful remote add --name=upstream ev4+ssh://ev4.example.com
  ```

## **Managing Tokens**

A token represents a set of credentials and permissions encoded as a compact, URL-safe string. When a user or service authenticates successfully with a KEK, EventfulDB issues a token that includes claims about the identity of the token holder and their authorized actions. This token is then used in subsequent requests to access protected resources or execute operations, eliminating the need for repeated authentication.

<aside>
💡 To mitigate the risk of exploitation, such as data spamming, writing operations in EventfulDB require tokens. Each token is associated with a specific write limit. Once this limit is reached, or a certain time period has elapsed, further writing is prohibited. A new token must then be generated for subsequent requests. It's important to note that EventfulDB does not utilize a traditional username and password system; instead, authentication is outsourced to the system integrating with EventfulDB.

</aside>

- **`token:auth`:** Authenticate and keeping the session alive

  ```toml
  # authenticate and keep session open for future requests
  eventful token:auth <auth-key> --keep-alive
  ```

- **`token:generate`**: Creates a new access token for authentication and authorization within the EventualDB system.
  ```toml

  # generate a token with expiration date and request horizon,
  # whichever occur first.
  eventful token:generate
  				--expiration=5000 --limit=10
  				--identifierType=<provider>
  				--identifierId=<xxx>

  # generate an auth key
  eventful token:generate --master-key=xxx --auth-key
  ```
- **`token:revoke`**: Invalidates an existing token, removing its access privileges to secure the system.
  ```toml
  # revoke a token
  eventful token:revoke --token=xxx

  # revoke an auth key
  eventful token:revoke --auth-key=xxx
  ```
- **`token:list`**: Lists all active tokens, providing an overview of current access permissions.
  ```toml
  # list active token
  eventful token:list cursor --take=xx --active

  # list tokens
  eventful token:list  cursor --take=xx --inactive

  # see a list of auth keys
  eventful token:list --kek --auth-key=xxx
  ```
- **`token:update`**: Updates the permissions or details associated with an existing auth-key.
  ```toml
  # update auth-key permissions
  eventful token:update <auth-key> --p1=x1 --p2=x2 --p3=x3
  ```
- **`token:refresh`**: Refreshes an access token, extending its validity without altering its permissions.
  ```toml
  # increase expiration by x seconds and limit by x number
  eventful token:refresh <access-token> --expiration=xx --limit=xxx
  ```

## **Schema Operations**

A schema in EventfulDB is a formal definition that outlines the structure, constraints, and rules for data within the system, such as events and aggregates. It serves as a blueprint for ensuring data integrity and consistency by specifying the allowed format and content for stored information

- **`schema:create`**: Defines a new schema for events or aggregates, establishing the data structure and rules.
  ```toml
  # create a schema
  evenful schema:create
  							--aggregate=patient
  							--events=patient-updated, patient-created
  							--snapshot-threshold=25
  ```
- **`schema:alter`**: Modifies an existing schema to accommodate changes in data requirements or business logic.
  ```toml
  # Set the snapshot threshold to 25. Update the threshold to every 25 events.
  eventful schema:alter patient --snapshot-threshold=25

  # Lock the patient aggregate, preventing execution of additional events.
  eventful schema:alter patient --lock

  # Lock the birthdate field, disallowing updates.
  eventful schema:alter patient --field=birthdate --lock

  # Unlock the birthdate field, allowing updates.
  eventful schema:alter patient --field=birthdate --lock=false

  # add field for <event>
  eventful schema:alter patient --event=<event> -a fieldname

  # remove field for <event>
  eventful schema:alter patient -e <event> -r fieldname
  ```
- **`schema:remove`**: Deletes an event definition from an aggregate while leaving the remaining events untouched.
  ```toml
  # remove the person_updated event from the person aggregate
  eventful schema:remove person person_updated
  ```
- **`schema:list`**: Displays all defined schemas, allowing users to review the existing data models.
  ```toml
  # show a list of schemas available in the system.
  eventful schema:list
  ```
- **`schema:show`**: Retrieves detailed information about a specific schema, including its structure and constraints.
  ```toml
  # show the schema spec for the patient aggregate
  eventful schema:show patient
  ```
- **`schema:pull`**: Pull schema definition from remote sources. By default, —dry-run flag is enabled .
  ```toml
  # pull all schemas definition from origin
  eventful schema:pull origin

  # pull scheme <x> definition from origin
  eventful schema:pull origin <x>
  ```
- **`schema:push`**: Push schema definition to remote sources
  ```toml
  # push schemas definition to origin
  eventful schema:push --origin

  # push a schema definition to origin
  event schema:push origin <x>
  ```
- **`schema:migrate`**: Apply schemas change to the system, users can now utilize newly created / updated aggregates or events.
  ```toml
  # see what will change
  eventful migrate --dry-run

  # discard changes
  eventul migrate --discard

  # apply the changes
  eventful migrate -f
  ```

## **Working with Aggregates and Events**

### Aggregate

An aggregate in EventfulDB is a collection of related events that are treated as a single unit for the purpose of data manipulation and state management. Aggregates serve as the cornerstone of EventDB's event sourcing model, encapsulating a set of changes or actions that together represent the state of a domain entity.

- **`aggregate:apply`**: Applies an event to an aggregate, altering its state according to the event's data.
  ```toml
  # apply patient_added event
  eventful aggregate:apply <event1> --field1=data1 --field2=data2
  eventful aggregate:apply <event2> --field1=data1 --field2=data2
  eventful aggregate:apply <event3> --field1=data1 --field2=data2
  ```
- **`aggregate:list`**: Lists all aggregates, offering a snapshot of the system's current state entities.
  ```toml
  # display a list of aggregates
  eventful aggregate:list
  ```
- **`aggregate:commit`**: commit all events in order (like a transactions, all events must committed successful or rollback will occurred).
  ```toml
  # apply uncommit events to aggregates
  eventful aggregate:commit
  ```
- **`aggregate:get`**: Fetches detailed information about a specific aggregate, including its state and event history.
  ```toml
  # get the current state of an aggregate
  eventful aggregate:get <x>

  # get the state of an aggreate at version 10
  eventful aggregate:get <x> 10
  ```
- **`aggregate:replay`**: Reapplies past events to an aggregate, useful for rebuilding state or debugging.
  ```toml
  # replay recent 10 events for an aggregate instant
  eventful aggregate:replay <id> 10

  #replay an aggregate instant starting from version 0
  eventful aggregate:replay <id> --skip=0 --take=10
  ```
- **`aggregate:snapshot`**: Creates a point-in-time snapshot of an aggregate's state, optimizing future state reconstructions.
  ```toml
  # create a snapshot of an aggregate instant with comments
  eventful aggregate:snapshot <id> --comment="useful comments here"
  ```
- **`aggregate:archive`**: Moves an aggregate to archival storage, preserving its history while optimizing active storage.
  ```toml
  # archive an aggregate instant, locking it for future updates
  eventful aggregate:archive <xx> --comment="useful comments here"
  ```
- **`aggregate:restore`:** Moves an aggregate out of storage into memory, unlocking it for future updates
  ```toml
  # activate an aggregate instant
  eventful aggreate:restore <xxx> --coments "useful comments here"
  ```
- **`aggregate:verify`:** Verify that an instant of aggregate has not been tampered,
  ```toml
  # verify that aggregate id <1234>
  eventful aggregate:verify <123>
  ```

### **Events**

Events in EventfulDB are intrinsically linked to aggregates, with each event representing an individual state change or action that affects a specific aggregate. Aggregates are composed of these related events and are treated as a cohesive unit, encapsulating the sequence of changes that collectively define the state of a domain entity.

- **`event:add`:** Records a new event, capturing a state change within the system. Similar to (`aggretate:apply + aggregate:commit`)
  ```toml
  # apply an event
  eventful event:add patient-added
  					--field1=data1
  					--field2=data2
  					--field3=data3
  ```
- **`event:get`**: Retrieves detailed data about a specific event, including its type, payload, and timestamp.
  ```toml
  # retreive specific event detail
  eventful event:get <hash>
  ```
- **`event:archive`**: Transfers events in short term memory to long-term storage, maintaining system performance while preserving history.
  ```toml
  # archive all
  eventful event:archive --all

  # archive a specific aggregtate id <x>
  eventful event:archive <xxx>
  ```

## Working with Data

Local data in EventfulDB can synced with remote sources via push and pull command. This is feature is heavily inspired by Git.

- `data:push` : Push data to other eventful databases.
  ```toml
  # see what data will be pushed to origin
  eventful data:push origin
  or
  eventful data:push origin --dry-run

  # push data to origin
  eventful data:push origin -f --dry-run=false
  ```
- `data:pull` : Pull data from other eventful databases.
  ```toml
  # pull data from origin in dry mode
  eventful data:pull origin
  or
  eventful data:pull origin --dry-run

  # pull data from upstream, sync local database
  eventful data:pull upstream -f --dry-run=false

  # subcribe to changes in upstream and continously sync the database
  eventful data:pull upstream --subscribe

  # pull a specific aggregate from origin
  eventful data:pull origin <xxx>
  ```

## Plugins

Plugins in EventfulDB enable seamless connections between EventfulDB and external systems, services, or applications, enhancing its core functionalities and enabling a more comprehensive data ecosystem. Through plugins, EventfulDB can export data for analysis, trigger workflows in other systems, synchronize with external databases, and much more.

- **`plugin:config`**: Sets up or modify a plugin establishing a connection between EventfulDB and an external service, enabling data exchange or functionality extension.
  ```toml
  # config an
  eventful plugin:config <plugin> --option1=xxx --option2=xxx
  ```
- **`plugin:list`**: Lists all configured plugins, providing an overview of external connections.
  ```toml
  # display a list of plugins
  eventful plugin:list

  # dispay a list of active plugin
  eventful plugin:list --active
  ```
- **`plugin:remove`**: Remove a plugin, severing the connection between EventDB and an external service.
  ```toml
  # event must be disabled with the disable flag before remove
  eventful plugin:remove <event> --disable
  ```
- **`plugin:replay`**: Manually triggers a replay with an external service
  ```toml
  # trigger a replay for a specify aggregate instant
  eventful plugin:replay <plugin> <aggregate_id>

  # trigger replay for all instant of an aggregate
  eventful plugin:replay <aggegate> -f
  ```
- **`plugin:test`**: Verifies the setup of an integration, ensuring that EventDB can communicate effectively with the external service.
  ```toml
  # test that a plugin is working by sending a dump event
  eventful plugin:test <plugin>
  ```
- **`plugin:enable`**: Activates a configured plugin, enabling its operations within EventfulDB.
  ```toml
  # enable a plugin, establishing a connection with underling system
  eventful plugin:enable <plugin>
  ```
- **`plugin:disable`**: Temporarily turns off a plugin, halting its data exchange or functional impact on EventfulDB.
  ```toml
  # disable a plugin, disconnect the underlining system
  eventful plugin:disable <plugin>
  ```

## Contributing

### **Quick Start**

1. **Report Issues**: Found a bug or have a suggestion? [Open an issue](https://chat.openai.com/g/g-kvXdAN8VA-eventdb-guide/c/72d2fa39-c3e2-4e40-bd61-5694f7b82aab#) with detailed information.
2. **Contribute Code**:
   - **Fork & Clone**: Fork the EventfulDB repo and clone your fork.
   - **Branch**: Create a branch for your changes from **`develop`**.
   - **Develop**: Make your changes, adhering to our coding standards. Add or update tests as necessary.
   - **Commit**: Use [Conventional Commits](https://www.conventionalcommits.org/) for clear, structured commit messages (e.g., **`feat: add new feature`** or **`fix: correct a bug`**).
   - **Pull Request**: Submit a pull request (PR) against the **`develop`** branch of the original repo. Describe your changes and link to any related issues.

### **Guidelines**

- **Code Review**: Your PR will be reviewed by the team. Be open to feedback and make requested adjustments.
- **Merge**: Once approved, your PR will be merged into the project, and you'll be credited as a contributor.

## License

EventfulDB is licensed under the [MIT](https://github.com/thachp/eventdb/blob/HEAD/apps/system/) License.
