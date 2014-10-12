package edu.rosehulman.postcn.Exam1Postcn;

public enum JoinTag {
	CLASS_TAG(0),
	STUDENT_TAG(1);
	
	private final int id;
	JoinTag(int id) {this.id = id;}
	public int getValue() {return id;}
}
