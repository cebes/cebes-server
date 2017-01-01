package io.cebes.common

import org.scalatest.FunSuite

/**
  * Created by d066177 on 01/01/2017.
  */
class TagSuite extends FunSuite {

  test("simple cases") {
    val tag1 = Tag.fromString("simple-tag_ab.c")
    assert(tag1.name === "simple-tag_ab.c")
    assert(tag1.host === Some("simple-tag_ab.c"))
    assert(tag1.port.isEmpty)
    assert(tag1.server === Some("simple-tag_ab.c"))
    assert(tag1.version.isEmpty)
    assert(tag1.toString === "simple-tag_ab.c")

    val ex1 = intercept[IllegalArgumentException] {
      Tag.fromString("simple-tag_ab.cTA")
    }
    assert(ex1.getMessage.startsWith("Invalid tag expression"))

    // tag with version
    val tag2 = Tag.fromString("simple-tag:v1")
    assert(tag2.name === "simple-tag")
    assert(tag2.host === Some("simple-tag"))
    assert(tag2.port.isEmpty)
    assert(tag2.server === Some("simple-tag"))
    assert(tag2.version === Some("v1"))
    assert(tag2.toString === "simple-tag:v1")

    // with path and host
    val tag3 = Tag.fromString("simple-tag:9000/abc-sz")
    assert(tag3.name === "simple-tag:9000/abc-sz")
    assert(tag3.host === Some("simple-tag"))
    assert(tag3.port === Some(9000))
    assert(tag3.server === Some("simple-tag:9000"))
    assert(tag3.version.isEmpty)
    assert(tag3.toString === "simple-tag:9000/abc-sz")

    // with path and host and version
    val tag4 = Tag.fromString("simple-tag:9000/abc-sz:latest")
    assert(tag4.name === "simple-tag:9000/abc-sz")
    assert(tag4.host === Some("simple-tag"))
    assert(tag4.port === Some(9000))
    assert(tag4.server === Some("simple-tag:9000"))
    assert(tag4.version === Some("latest"))
    assert(tag4.toString === "simple-tag:9000/abc-sz:latest")

    // fuzzy case
    val tag5 = Tag.fromString("simple-tag:9000")
    assert(tag5.name === "simple-tag:9000")
    assert(tag5.host === Some("simple-tag"))
    assert(tag5.port === Some(9000))
    assert(tag5.server === Some("simple-tag:9000"))
    assert(tag5.version.isEmpty)
    assert(tag5.toString === "simple-tag:9000")

    val ex2 = intercept[IllegalArgumentException] {
      Tag.fromString("simple-tag:500/:v2")
    }
    assert(ex2.getMessage.startsWith("Invalid tag expression"))
  }
}
