import PersistentBankAccount.Command._
import PersistentBankAccount.Response._
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

import scala.util.{Failure, Success, Try}

object PersistentBankAccount {

  /*
     - fault tolerance
     - auditing
   */

  // commands = messages
  sealed trait Command
  object Command {
    case class CreateBankAccount(user: String, currency: String, initialBalance: Double, replyTo: ActorRef[Response]) extends Command
    case class UpdateBalance(id: String, currency: String, amount: Double /* can be < 0*/, replyTo: ActorRef[Response]) extends Command
    case class GetBankAccount(id: String, replyTo: ActorRef[Response]) extends Command
  }

  // events = to persist to Cassandra
  trait Event
  case class BankAccountCreated(bankAccount: BankAccount) extends Event
  case class BalanceUpdated(amount: Double) extends Event
  // state
  case class BankAccount(id: String, user: String, currency: String, balance: Double)

  // responses
  sealed trait Response
  object Response {
    case class BankAccountCreatedResponse(id: String) extends Response
    case class BankAccountBalanceUpdatedResponse(maybeBankAccount: Try[BankAccount]) extends Response
    case class GetBankAccountResponse(maybeBankAccount: Option[BankAccount]) extends Response
  }

  val commandHandler: (BankAccount, Command) => Effect[Event, BankAccount] = (state, command) => {
    command match {
      case CreateBankAccount(user, currency, initialBalance, replyTo) =>
        val id = state.id
        Effect.persist(BankAccountCreated(BankAccount(id, user, currency, initialBalance )))
        .thenReply(replyTo)(_ => BankAccountCreatedResponse(id))
      case UpdateBalance(id, currency, amount, replyTo) =>
        val newBalance = state.balance + amount
        if(newBalance < 0 ){
          Effect.reply(replyTo)(BankAccountBalanceUpdatedResponse(Failure(new RuntimeException("Cannot withdraw more than available"))))
        } else {
          Effect.persist(BalanceUpdated(newBalance))
            .thenReply(replyTo)(newState => BankAccountBalanceUpdatedResponse(Success(newState)))
        }
      case GetBankAccount(_, replyTo) =>
        Effect.reply(replyTo)(GetBankAccountResponse(Some(state)))
    }
  }

  val eventHandler: (BankAccount, Event) => BankAccount = (state, event) => {
    event match {
      case BalanceUpdated(amount) => state.copy(balance = state.balance + amount)
      case BankAccountCreated(bankAccount) => bankAccount
    }
  }

  def apply(id :String): Behavior[Command] = {
    EventSourcedBehavior[Command, Event, BankAccount](
      persistenceId = PersistenceId.ofUniqueId(id),
      emptyState = BankAccount(id, "", "", 0.0),
      commandHandler,
      eventHandler
    )
  }
}
