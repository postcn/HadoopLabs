package edu.rosehulman.postcn.Lab3ReduceSideJoin;

public enum JoinTag {
	SPRINT_TAG(0),
	WORKER_TAG(1);
	
	private final int id;
	JoinTag(int id) {this.id = id;}
	public int getValue() {return id;}
}
