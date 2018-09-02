package com.bittiger.querypool;

import java.util.StringTokenizer;

//sql.getNewProducts
public class bq2 implements QueryMetaData {
	public String query = "SELECT i_id, i_title, a_fname, a_lname " + "FROM item, author "
			+ "WHERE item.i_a_id = author.a_id " + "AND (item.i_subject like '%?%') "
			+ "ORDER BY item.i_pub_date DESC,item.i_title " + "limit 50";
	// hive rewrite:
	// in the inner subquery, include a column item.i_pub_date
	// in the outer query, exclude the column
	// public String query = "SELECT i_id, i_title, a_fname, a_lname from " +
	// "(SELECT i_id, item.i_title, a_fname, a_lname, item.i_pub_date " +
	// "FROM item, author " +
	// "WHERE item.i_a_id = author.a_id " +
	// "AND (item.i_subject like '%?%') " +
	// "ORDER BY item.i_pub_date DESC,item.i_title " +
	// "limit 50)subq";
	ParmGenerator pg = new ParmGenerator();

	public String getQueryStr() {
		String qString = "";
		int count = 0;
		StringTokenizer st = new StringTokenizer(query, "?", false);
		while (st.hasMoreTokens()) {
			qString += st.nextToken();
			count++;
			switch (count) {
			case 1:
				qString += pg.getItemSubject();
				break;
			case 2:
				break;
			default:
				System.out.println("More token than expected");
				System.exit(100);
			}
		}
		return qString;
	}
}
