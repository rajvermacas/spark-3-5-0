// AccessModifierOverrideTest.scala
// Demonstrates access modifier overriding in Scala

// Parent class with methods of different access levels
class Parent {
  private def privateMethod(): String = {
    "Parent's private method"
  }
  
  protected def protectedMethod(): String = {
    "Parent's protected method"
  }
  
  def publicMethod(): String = {
    "Parent's public method"
  }
  
  // Method to access private method for demonstration
  def accessPrivate(): String = {
    privateMethod()
  }
}

// Child class demonstrating various access modifier scenarios
class Child extends Parent {
  // Cannot override private methods - they're not visible to subclasses
  // This is a new method, not an override
  private def privateMethod(): String = {
    "Child's private method (not an override)"
  }
  
  // Can override protected method with same access level
  override protected def protectedMethod(): String = {
    "Child's protected override of protected method"
  }
  
  // Can override public method with same access level
  override def publicMethod(): String = {
    "Child's override of public method"
  }
  
  // Method to access our own private method
  def accessChildPrivate(): String = {
    privateMethod()
  }
  
  // Method to access parent's protected method
  def accessProtected(): String = {
    protectedMethod()  // Calls the overridden version in Child
  }
}

// Child class demonstrating widening access modifiers
class ChildWithWiderAccess extends Parent {
  // Can override protected method with public (widening access)
  override def protectedMethod(): String = {
    "Child's public override of protected method"
  }
}

// Main object to run the test
object AccessModifierOverrideTest {
  def main(args: Array[String]): Unit = {
    println("Testing access modifier overriding in Scala")
    println("------------------------------------------")
    
    val parent = new Parent()
    val child = new Child()
    val widerChild = new ChildWithWiderAccess()
    
    println("1. Basic method overriding:")
    println("   Parent's public method: " + parent.publicMethod())
    println("   Child's public method: " + child.publicMethod())
    
    println("\n2. Private methods:")
    println("   Parent's private method (accessed internally): " + parent.accessPrivate())
    println("   Child's private method (accessed internally): " + child.accessChildPrivate())
    println("   Note: Child cannot override Parent's private method as it's not visible")
    
    println("\n3. Protected methods:")
    println("   Child accessing its overridden protected method: " + child.accessProtected())
    
    println("\n4. Widening access (protected → public):")
    println("   ChildWithWiderAccess's public method (was protected in Parent): " + widerChild.protectedMethod())
    println("   Note: This demonstrates that protected methods can be overridden as public")
    
    println("\n5. Narrowing access is not allowed:")
    println("   Note: Cannot override a public method as protected or private")
    println("   Note: Cannot override a protected method as private")
    
    println("\nDone testing")
  }
}
