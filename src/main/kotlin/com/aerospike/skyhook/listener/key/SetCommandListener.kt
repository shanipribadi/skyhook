package com.aerospike.skyhook.listener.key

import com.aerospike.client.AerospikeException
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.ResultCode
import com.aerospike.client.Value
import com.aerospike.client.listener.WriteListener
import com.aerospike.client.policy.RecordExistsAction
import com.aerospike.client.policy.WritePolicy
import com.aerospike.skyhook.command.RedisCommand
import com.aerospike.skyhook.command.RequestCommand
import com.aerospike.skyhook.listener.BaseListener
import com.aerospike.skyhook.listener.ValueType
import com.aerospike.skyhook.util.Typed
import io.netty.channel.ChannelHandlerContext
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class SetCommandListener(
    ctx: ChannelHandlerContext
) : BaseListener(ctx), WriteListener {

    @Volatile
    private lateinit var command: RedisCommand

    private data class Params(val writePolicy: WritePolicy, val value: Value)

    override fun handle(cmd: RequestCommand) {
        command = cmd.command
        val key = createKey(cmd.key)
        val params = parse(cmd)
        client.put(
            null, this, params.writePolicy, key,
            Bin(aeroCtx.bin, params.value), *systemBins(ValueType.STRING)
        )
    }

    override fun onSuccess(key: Key?) {
        try {
            if (command == RedisCommand.SETNX) {
                writeLong(1L)
            } else {
                writeOK()
            }
            flushCtxTransactionAware()
        } catch (e: Exception) {
            closeCtx(e)
        }
    }

    override fun writeError(e: AerospikeException?) {
        if (command == RedisCommand.SETNX) {
            if (e?.resultCode == ResultCode.KEY_EXISTS_ERROR) {
                writeLong(0L)
            } else {
                writeErrorString(e?.message)
            }
        } else {
            when (e?.resultCode) {
                ResultCode.KEY_EXISTS_ERROR -> {
                    writeNullString()
                }
                ResultCode.KEY_NOT_FOUND_ERROR -> {
                    writeNullString()
                }
                else -> {
                    writeErrorString(e?.message)
                }
            }
        }
    }

    private fun parse(cmd: RequestCommand): Params {
        val writePolicy = getWritePolicy()
        // REDIS SET by default reset expiry to no expiry
        // we leave this to 0 so that default-ttl gets applied
        // https://docs.aerospike.com/docs/operations/manage/storage/#setting-time-to-live-ttl-for-data-expiration
        // writePolicy.expiration = -1
        return when (cmd.command) {
            RedisCommand.SET -> {
                require(cmd.argCount >= 3 && cmd.argCount <= 6) { argValidationErrorMsg(cmd) }
                var i = 3
                while (i < cmd.argCount) {
                    val v = String(cmd.args[i]).toUpperCase(Locale.ENGLISH)
                    when (v) {
                        "EX" -> {
                            i++
                            val exp = Typed.getInteger(cmd.args[i])
                            require(exp > 0) { "invalid expire time in set" }
                            writePolicy.expiration = exp
                        }
                        "PX" -> {
                            i++
                            val expMs = Typed.getLong(cmd.args[i])
                            // round up to multiples of 1 second
                            val exp = ((expMs + 999) / 1000).toInt()
                            require(exp > 0) { "invalid expire time in set" }
                            writePolicy.expiration = exp
                        }
                        "EXAT" -> {
                            i++
                            val exp = Instant.now().until(Instant.ofEpochSecond(Typed.getLong(cmd.args[1])), ChronoUnit.SECONDS).toInt()
                            require(exp > 0) { "invalid expire time in set" }
                            writePolicy.expiration = exp
                        }
                        "PXAT" -> {
                            i++
                            val expMs = Instant.now().until(Instant.ofEpochMilli(Typed.getLong(cmd.args[1])), ChronoUnit.MILLIS)
                            // round up to multiples of 1 second
                            val exp = ((expMs + 999) / 1000).toInt()
                            require(exp > 0) { "invalid expire time in set" }
                            writePolicy.expiration = exp
                        }
                        "KEEPTTL" -> {
                            writePolicy.expiration = -2
                        }
                        "NX" -> writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY
                        "XX" -> writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY
                        else -> {
                            throw IllegalArgumentException("syntax error")
                        }
                    }
                    i++
                }

                Params(writePolicy, Typed.getValue(cmd.args[2]))
            }
            RedisCommand.SETNX -> {
                require(cmd.argCount == 3) { argValidationErrorMsg(cmd) }

                writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY
                Params(writePolicy, Typed.getValue(cmd.args[2]))
            }
            RedisCommand.SETEX -> {
                require(cmd.argCount == 4) { argValidationErrorMsg(cmd) }

                val exp = Typed.getInteger(cmd.args[2])
                require(exp > 0) { "invalid expire time in setex" }
                writePolicy.expiration = exp
                Params(writePolicy, Typed.getValue(cmd.args[3]))
            }
            RedisCommand.PSETEX -> {
                require(cmd.argCount == 4) { argValidationErrorMsg(cmd) }

                val expMs = Typed.getLong(cmd.args[2])
                // round up to multiples of 1 second
                val exp = ((expMs + 999) / 1000).toInt()
                require(exp > 0) { "invalid expire time in psetex" }
                writePolicy.expiration = exp
                Params(writePolicy, Typed.getValue(cmd.args[3]))
            }
            else -> {
                throw IllegalArgumentException(cmd.command.toString())
            }
        }
    }
}
