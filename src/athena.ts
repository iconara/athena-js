import {AwsCredentialIdentityProvider} from '@aws-sdk/types'
import {AthenaClient, GetQueryExecutionCommand, GetQueryExecutionCommandOutput, GetQueryResultsCommand, QueryExecution, QueryExecutionStatus, StartQueryExecutionCommand} from '@aws-sdk/client-athena'

export interface Row {
  columnNames: string[]
  values: string[]
}

type ResultPage = {
  nextToken?: string
  columnNames: string[]
  rows: string[][]
}

export class AthenaError extends Error {
  constructor(message: string) {
    super(message)
  }
}

export class QueryError extends AthenaError {
  #status: QueryExecutionStatus

  constructor(status: QueryExecutionStatus) {
    super(status.StateChangeReason!)
    this.#status = status
  }

  get category(): number | undefined {
    return this.#status.AthenaError?.ErrorCategory
  }

  get type(): number | undefined {
    return this.#status.AthenaError?.ErrorType
  }
}

export class ResultSet implements AsyncIterable<Row> {
  #client: AthenaClient
  #queryExecutionId: string
  #queryExecution: QueryExecution

  constructor(client: AthenaClient, queryExecutionId: string, queryExecution: QueryExecution) {
    this.#client = client
    this.#queryExecutionId = queryExecutionId
    this.#queryExecution = queryExecution
  }

  get outputLocation(): string {
    return this.#queryExecution.ResultConfiguration!.OutputLocation!
  }

  async #loadPage(nextToken: string | undefined): Promise<ResultPage> {
    const nextPageCommand = new GetQueryResultsCommand({
      QueryExecutionId: this.#queryExecutionId,
      NextToken: nextToken,
    })
    const queryResults = await this.#client.send(nextPageCommand)
    const rows = queryResults.ResultSet!.Rows!.map((row) => row.Data!.map((cell) => cell.VarCharValue!))
    const columnNames = queryResults.ResultSet!.ResultSetMetadata!.ColumnInfo!.map((columnInfo) => columnInfo.Label!)
    return {
      nextToken: queryResults.NextToken,
      columnNames,
      rows,
    }
  }

  [Symbol.asyncIterator](): AsyncGenerator<Row, void, undefined> {
    const self = this
    return (async function * () {
      let headerSkipped = false
      let currentPage: ResultPage | undefined = undefined
      do {
        currentPage = await self.#loadPage(undefined)
        const {columnNames, rows} = currentPage
        if (!headerSkipped) {
          rows.shift()
          headerSkipped = true
        }
        yield* rows.map((values) => ({columnNames, values})) as Row[]
      } while (currentPage.nextToken !== undefined)
    }())
  }
}

export class Athena {
  static TERMINAL_STATES = ['SUCCEEDED', 'FAILED', 'CANCELED']

  #client: AthenaClient
  #workGroup: string

  constructor(region: string, credentials: AwsCredentialIdentityProvider, workGroup: string) {
    this.#client = new AthenaClient({region, credentials})
    this.#workGroup = workGroup
  }

  async query(sql: string): Promise<ResultSet> {
    const startCommand = new StartQueryExecutionCommand({
      WorkGroup: 'tolv',
      QueryString: sql,
      ResultReuseConfiguration: {
        ResultReuseByAgeConfiguration: {
          Enabled: true,
          MaxAgeInMinutes: 60 * 24 * 7,
        }
      },
    })
    const {QueryExecutionId: queryExecutionId} = await this.#client.send(startCommand)
    let queryStatus: GetQueryExecutionCommandOutput | undefined = undefined
    const statusCommand = new GetQueryExecutionCommand({QueryExecutionId: queryExecutionId})
    let delay = 100
    while (queryStatus === undefined || !Athena.TERMINAL_STATES.includes(queryStatus.QueryExecution?.Status?.State || '')) {
      await new Promise((resolve) => setTimeout(resolve, delay))
      delay = Math.max(delay * 1.2, 2000)
      queryStatus = await this.#client.send(statusCommand)
    }
    if (queryStatus.QueryExecution?.Status?.State === 'SUCCEEDED') {
      return new ResultSet(this.#client, queryExecutionId!, queryStatus.QueryExecution!)
    } else {
      if (queryStatus.QueryExecution?.Status !== undefined) {
        throw new QueryError(queryStatus.QueryExecution?.Status)
      } else {
        throw new AthenaError(`Query failed but could not retrieve status`)
      }
    }
  }
}
