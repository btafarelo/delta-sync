package com.blogspot.btafarelo.deltasync;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import com.nothome.delta.Delta;
import com.nothome.delta.GDiffPatcher;

public class Main {

	public static void main(String[] args) throws Exception {
		connectDB1();
		
		BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(
				"db1/db1.h2.db"));

		ByteArrayOutputStream array = new ByteArrayOutputStream();
		byte[] bytes = new byte[128];

		while (bIn.read(bytes) != -1) {
			array.write(bytes);
			bytes = new byte[128];
		}

		bIn.close();

		byte target[] = array.toByteArray();

		bIn = new BufferedInputStream(new FileInputStream("db2/db2.h2.db"));

		array = new ByteArrayOutputStream();
		bytes = new byte[128];

		while (bIn.read(bytes) != -1) {
			array.write(bytes);
			bytes = new byte[128];
		}

		bIn.close();

		byte source[] = array.toByteArray();

		Delta d = new Delta();

		byte patch[] = d.compute(source, target);

		GDiffPatcher p = new GDiffPatcher();

		byte patchedSource[] = p.patch(source, patch);

		System.out.println("patch len = " + patch.length);

		BufferedOutputStream bOut = new BufferedOutputStream(
				new FileOutputStream(new File("db2/db2.h2.db")));
		bOut.write(patchedSource);
		bOut.close();

		connectDB2();
	}
	
	private static void connectDB1() throws Exception {
		Class.forName("org.h2.Driver");

		Connection cnn1 = DriverManager.getConnection("jdbc:h2:file:db1/db1");

		//cnn1.createStatement().execute("create table tbl1 (id int not null, name varchar(50) not null);");

		cnn1.createStatement().execute(
				"insert into tbl1 values (3, 'Luciana Pereira')");

		cnn1.close();	
	}
	
	private static void connectDB2() throws Exception {
		Connection cnn2 =
				DriverManager.getConnection("jdbc:h2:file:db2/db2");
				
		ResultSet rs = cnn2.createStatement().executeQuery("select * from tbl1");
		
		while (rs.next()) {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				System.out.print(rs.getString(i) + " - ");				
			}
			
			System.out.println();
		}
		
		cnn2.close();
	}
}
