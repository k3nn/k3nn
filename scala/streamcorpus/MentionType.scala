/**
 * generated by Scrooge ${project.version}
 */
package streamcorpus

import com.twitter.scrooge.ThriftEnum


@javax.annotation.Generated(value = Array("com.twitter.scrooge.Compiler"), date = "2013-08-12T15:12:53.355-0400")
case object MentionType {
  
  case object Name extends MentionType {
    val value = 0
    val name = "Name"
  }
  
  case object Pro extends MentionType {
    val value = 1
    val name = "Pro"
  }
  
  case object Nom extends MentionType {
    val value = 2
    val name = "Nom"
  }

  /**
   * Find the enum by its integer value, as defined in the Thrift IDL.
   * @throws NoSuchElementException if the value is not found.
   */
  def apply(value: Int): MentionType = {
    value match {
      case 0 => Name
      case 1 => Pro
      case 2 => Nom
      case _ => throw new NoSuchElementException(value.toString)
    }
  }

  /**
   * Find the enum by its integer value, as defined in the Thrift IDL.
   * Returns None if the value is not found
   */
  def get(value: Int): Option[MentionType] = {
    value match {
      case 0 => scala.Some(Name)
      case 1 => scala.Some(Pro)
      case 2 => scala.Some(Nom)
      case _ => scala.None
    }
  }

  def valueOf(name: String): Option[MentionType] = {
    name.toLowerCase match {
      case "name" => scala.Some(MentionType.Name)
      case "pro" => scala.Some(MentionType.Pro)
      case "nom" => scala.Some(MentionType.Nom)
      case _ => scala.None
    }
  }

  lazy val list: List[MentionType] = scala.List[MentionType](
    Name,
    Pro,
    Nom
  )
}



@javax.annotation.Generated(value = Array("com.twitter.scrooge.Compiler"), date = "2013-08-12T15:12:53.355-0400")
sealed trait MentionType extends ThriftEnum with Serializable