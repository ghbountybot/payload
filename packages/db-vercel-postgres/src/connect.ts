import type { DrizzleAdapter } from '@payloadcms/drizzle/types'
import type { Connect } from 'payload'

import { pushDevSchema } from '@payloadcms/drizzle'
import { sql, VercelPool } from '@vercel/postgres'
import { drizzle } from 'drizzle-orm/node-postgres'
import pg from 'pg'

import type { VercelPostgresAdapter } from './types.js'

const connectWithReconnect = async function ({
  adapter,
  client,
  reconnect = false,
}: {
  adapter: VercelPostgresAdapter
  client: pg.Pool | VercelPool
  reconnect?: boolean
}) {
  let result

  if (!reconnect) {
    result = await client.connect()
  } else {
    try {
      result = await client.connect()
    } catch (ignore) {
      setTimeout(() => {
        adapter.payload.logger.info('Reconnecting to postgres')
        void connectWithReconnect({ adapter, client, reconnect: true })
      }, 1000)
    }
  }

  if (!result) {return}

  // Handle both pg.Pool and VercelPool error events
  result.on('error', (err) => {
    try {
      // Handle various connection errors
      if (
        err.code === 'ECONNRESET' ||
        err.message?.includes('Connection terminated unexpectedly') ||
        err.message?.includes('connection terminated')
      ) {
        void connectWithReconnect({ adapter, client, reconnect: true })
      }
    } catch (ignore) {
      // swallow error
    }
  })
}

export const connect: Connect = async function connect(
  this: VercelPostgresAdapter,
  options = {
    hotReload: false,
  },
) {
  const { hotReload } = options

  this.schema = {
    pgSchema: this.pgSchema,
    ...this.tables,
    ...this.relations,
    ...this.enums,
  }

  try {
    const logger = this.logger || false
    let client: pg.Pool | VercelPool

    const connectionString = this.poolOptions?.connectionString ?? process.env.POSTGRES_URL

    // Use non-vercel postgres for local database
    if (
      !this.forceUseVercelPostgres &&
      connectionString &&
      ['127.0.0.1', 'localhost'].includes(new URL(connectionString).hostname)
    ) {
      client = new pg.Pool(
        this.poolOptions ?? {
          connectionString,
        },
      )
    } else {
      client = this.poolOptions ? new VercelPool(this.poolOptions) : sql
    }

    // Initialize connection with reconnect logic
    await connectWithReconnect({ adapter: this, client })

    // Passed the poolOptions if provided,
    // else have vercel/postgres detect the connection string from the environment
    this.drizzle = drizzle({
      client,
      logger,
      schema: this.schema,
    })

    if (!hotReload) {
      if (process.env.PAYLOAD_DROP_DATABASE === 'true') {
        this.payload.logger.info(`---- DROPPING TABLES SCHEMA(${this.schemaName || 'public'}) ----`)
        await this.dropDatabase({ adapter: this })
        this.payload.logger.info('---- DROPPED TABLES ----')
      }
    }
  } catch (err) {
    if (err.message?.match(/database .* does not exist/i) && !this.disableCreateDatabase) {
      // capitalize first char of the err msg
      this.payload.logger.info(
        `${err.message.charAt(0).toUpperCase() + err.message.slice(1)}, creating...`,
      )
      const isCreated = await this.createDatabase()

      if (isCreated) {
        await this.connect(options)
        return
      }
    } else {
      this.payload.logger.error({
        err,
        msg: `Error: cannot connect to Postgres. Details: ${err.message}`,
      })
    }

    if (typeof this.rejectInitializing === 'function') {
      this.rejectInitializing()
    }
    process.exit(1)
  }

  await this.createExtensions()

  // Only push schema if not in production
  if (
    process.env.NODE_ENV !== 'production' &&
    process.env.PAYLOAD_MIGRATING !== 'true' &&
    this.push !== false
  ) {
    await pushDevSchema(this as unknown as DrizzleAdapter)
  }

  if (typeof this.resolveInitializing === 'function') {
    this.resolveInitializing()
  }

  if (process.env.NODE_ENV === 'production' && this.prodMigrations) {
    await this.migrate({ migrations: this.prodMigrations })
  }
}
