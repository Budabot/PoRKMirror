package com.jkbff.ao.porkmirror

import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Created by Jason on 5/16/2015.
 */
@RunWith(classOf[JUnitRunner])
class ProgramTest extends FunSuite {
	test("cleanXMLTags") {
		val xml = "<root><organization_name><Osos Furiosos></organization_name></root>"
		val expected = "<root><organization_name>&lt;Osos Furiosos&gt;</organization_name></root>"

		val result = Program.cleanXMLTags(xml, Seq("organization_name"))

		Assert.assertEquals(expected, result)
	}

	test("cleanXMLTags tag does not exist") {
		val xml = "<root><organization_name><Osos Furiosos></organization_name></root>"

		val result = Program.cleanXMLTags(xml, Seq("name"))

		Assert.assertEquals(xml, result)
	}
}
